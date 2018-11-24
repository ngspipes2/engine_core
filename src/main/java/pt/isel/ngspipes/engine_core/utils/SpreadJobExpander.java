package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.entities.factories.JobFactory;
import pt.isel.ngspipes.engine_core.entities.factories.PipelineFactory;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class SpreadJobExpander {

    static class InOutput {
        Input input;
        BiFunction<String, Job, List<String>> getOutputValues;

        InOutput(Input input, BiFunction<String, Job, List<String>> getOutputValues) {
            this.input = input;
            this.getOutputValues = getOutputValues;
        }
    }

    public static void expandSpreadJob(Pipeline pipeline, SimpleJob job, Collection<ExecutionNode> graph,
                                       BiFunction<String, Job, List<String>> getOutValues) throws EngineException {

        List<Job> chainsFrom = job.getChainsFrom();
        List<Job> spreadJobs = getExpandedJobs(pipeline, job, getOutValues);
        addSpreadNodes(pipeline, job, graph, chainsFrom, spreadJobs);
    }

    public static boolean isResourceType(String type) {
        boolean isFile = type.contains("ile");
        boolean directory = type.contains("irectory");
        return isFile || directory;
    }

    public static List<Job> getExpandedJobs(Pipeline pipeline, SimpleJob job, BiFunction<String, Job, List<String>> getOutValues) throws EngineException {
        List<Job> spreadJobs = new LinkedList<>();
        addSpreadJobs(job, spreadJobs, pipeline, getOutValues);
        return spreadJobs;
    }

    public static void addSpreadJobChilds(Pipeline pipeline, Job job, List<Job> chainsFrom,
                                          List<ExecutionNode> childs, int idx) {
        for (Job chainJob : chainsFrom) {
            if (chainJob.getSpread() != null) {
                addSpreadChild(pipeline, job, childs, chainJob, idx);
            } else {
                chainJob.setInconclusive(!chainJob.isInconclusive());
            }
        }
    }

    public static void addSpreadJobs(Job job, List<Job> jobs, Pipeline pipeline,
                                     BiFunction<String, Job, List<String>> getOutValues) throws EngineException {
        Spread spread = job.getSpread();
        BiFunction<InOutput, Pipeline, List<String>> values;

        if (isSpreadChain(job, pipeline)) {
            values = SpreadJobExpander::getMiddleInputValues;
        } else {
            values = SpreadJobExpander::getInitInputValues;
        }

        Map<String, List<String>> valuesOfInputsToSpread = getInputValuesToSpread(job, pipeline, values, getOutValues);
        if (spread.getStrategy() != null)
            SpreadCombiner.getInputsCombination(spread.getStrategy(), valuesOfInputsToSpread);

        int idx = 0;
        int len = getInputValuesLength(valuesOfInputsToSpread);

        while (idx < len) {
            Job jobToSpread = getSpreadJob(job, valuesOfInputsToSpread, idx, pipeline);
            jobToSpread.setParents(job.getParents());
            jobs.add(jobToSpread);
            idx++;
        }
    }

    public static List<String> getValues(String inputValue) {
        inputValue = inputValue.replace("[", "");
        inputValue = inputValue.replace("]", "");

        String[] split = inputValue.split(",");
        List<String> inputsValues = new LinkedList<>();

        for (String str : split)
            inputsValues.add(str.trim());

        return inputsValues;
    }



    private static List<String> getMiddleInputValues(InOutput inOut, Pipeline pipeline) {
        Input input = inOut.input;
        String chainOutput = input.getChainOutput();
        if (chainOutput != null && !chainOutput.isEmpty() && isResourceType(input.getType())) {
            Job originJob = input.getOriginJob()  == null ? pipeline.getJobById(chainOutput) : input.getOriginJob();
            return inOut.getOutputValues.apply(chainOutput, originJob);
        } else
            return getInitInputValues(inOut, pipeline);
    }

    private static List<String> getInitInputValues(InOutput inOut, Pipeline pipeline) {
        String inputValue = inOut.input.getValue();
        return getValues(inputValue);
    }

    private static boolean usedBySameType(Job job, Output output, String type) {
        if (output.getType().equalsIgnoreCase("string")) {
            List<String> usedBy = output.getUsedBy();
            for (String uses : usedBy) {
                if (job.getOutputById(uses).getType().equalsIgnoreCase(type))
                    return true;
            }
        }
        return false;
    }

    private static void updateJoinJobChainInputValues(Job job) throws EngineException {
        // update in values
        List<Input> spreadChainInputs = new LinkedList<>();
        job.getInputs().forEach((i) -> { if (i.getOriginJob().getSpread() != null) spreadChainInputs.add(i); });

        for (Input in : spreadChainInputs) {
            String type = in.getType();
            if (type.equalsIgnoreCase("directory")) {
                in.setValue(in.getOriginJob().getEnvironment().getOutputsDirectory());
            } else if (!type.equalsIgnoreCase("file[]")
                    || !type.equalsIgnoreCase("string")) {
                throw new EngineException("Input " + in.getName() + " value for incoming spread result and type "
                        + type + "not supported.");
            }
        }
    }

    private static List<Output> getCopyOfJobOutputs(List<Output> outputs, SimpleJob job) {
        List<Output> outs = new LinkedList<>();
        for (Output output : outputs) {
            Output out = new Output(output.getName(), job, output.getType(), output.getValue());
            outs.add(out);
        }
        return outs;
    }

    private static List<Input> getCopyOfJobInputs(List<Input> jobInputs, Map<String, List<String>> inputs,
                                                    int idx, Job job, Pipeline pipeline, String baseJob) {
        List<Input> copiedInputs = new LinkedList<>();
        for (Input input : jobInputs) {
            List<Input> subInputs = input.getSubInputs();
            String originStep = input.getOriginStep();
            Job originJob = originStep != null ? originStep.equals(baseJob) ? job : pipeline.getJobById(originStep) : job;
            List<Input> copyOfJobInputs = subInputs == null ? new LinkedList<>() : getCopyOfJobInputs(subInputs, inputs, idx, job, pipeline, baseJob);
            String name = input.getName();
            String value = getValue(inputs, idx, input, name);
            copiedInputs.add(new Input(name, originJob, input.getChainOutput(), input.getType(), value, input.getPrefix(),
                    input.getSeparator(), input.getSuffix(), copyOfJobInputs));
        }
        return copiedInputs;
    }

    private static String getValue(Map<String, List<String>> inputs, int idx, Input input, String name) {
        String value;
        if (inputs.containsKey(name))
            value = inputs.get(name).get(idx);
        else
            value = input.getValue();
        return value;
    }

    private static Environment copyEnvironment(Job job, String stepId, String workingDirectory) {
        Environment environment = new Environment();

        Environment baseEnvironment = job.getEnvironment();
        if (baseEnvironment == null)
            baseEnvironment = JobFactory.getJobEnvironment(job.getId(), workingDirectory);
        environment.setDisk(baseEnvironment.getDisk());
        environment.setMemory(baseEnvironment.getMemory());
        environment.setCpu(baseEnvironment.getCpu());
        environment.setWorkDirectory(baseEnvironment.getWorkDirectory() + File.separatorChar + stepId);
        environment.setOutputsDirectory(baseEnvironment.getOutputsDirectory() + File.separatorChar + stepId);

        return environment;
    }

    private static int getInputValuesLength(Map<String, List<String>> valuesOfInputsToSpread) {
        if (valuesOfInputsToSpread.values().iterator().hasNext())
            return valuesOfInputsToSpread.values().iterator().next().size();
        return 0;
    }

    private static Map<String, List<String>> getInputValuesToSpread(Job job, Pipeline pipeline,
                                                                    BiFunction<InOutput, Pipeline, List<String>> getValues,
                                                                    BiFunction<String, Job, List<String>> getOutValues) {
        Map<String, List<String>> valuesOfInputsToSpread = new HashMap<>();
        List<String> inputsToSpread = (List<String>) job.getSpread().getInputs();

        InOutput inOut = new InOutput(null, getOutValues);

        for (Input input : job.getInputs()) {
            inOut.input = input;
            if (inputsToSpread.contains(input.getName())) {
                valuesOfInputsToSpread.put(input.getName(), getValues.apply(inOut, pipeline));
            }
        }

        return valuesOfInputsToSpread;
    }

    private static boolean isSpreadChain(Job job, Pipeline pipeline) {
        if (job.getChainsTo().isEmpty())
            return false;

        for (Input in : job.getInputs()) {
            if (in.getOriginStep() != null && !in.getOriginStep().equals(job.getId())) {
                if (pipeline.getJobById(in.getOriginStep()).getSpread() != null)
                    return true;
            }
        }
        return false;
    }

    private static SimpleJob getSpreadJob(Job job, Map<String, List<String>> inputs, int idx, Pipeline pipeline) {
        if (job instanceof SimpleJob) {
            SimpleJob simpleJob = (SimpleJob) job;
            ExecutionContext executionContext = simpleJob.getExecutionContext();

            String id = job.getId() + "_" + job.getId() + idx;
            Environment env = copyEnvironment(job, id, pipeline.getEnvironment().getWorkDirectory());
            SimpleJob sJob = new SimpleJob(id, env, simpleJob.getCommand(), executionContext);
            List<Input> jobInputs = getCopyOfJobInputs(job.getInputs(), inputs, idx, simpleJob, pipeline, job.getId());
            sJob.setInputs(jobInputs);
            List<Output> jobOutputs = getCopyOfJobOutputs(job.getOutputs(), simpleJob);
            sJob.setOutputs(jobOutputs);
            sJob.setParents(job.getParents());
            simpleJob.getChainsFrom().forEach(sJob::addChainsFrom);
            return sJob;
        }
        //falta compose jobs
        throw new NotImplementedException();
    }

    private static void addSpreadNodes(Pipeline pipeline, SimpleJob job, Collection<ExecutionNode> graph, List<Job> chainsFrom, List<Job> spreadJobs) {
        for (int idx = 0; idx < spreadJobs.size(); idx++) {
            Job spreadJob = spreadJobs.get(idx);
            List<ExecutionNode> childs = new LinkedList<>();
            spreadJob.setParents(job.getChainsFrom().stream().map(Job::getId).collect(Collectors.toList()));
            ExecutionNode node = new ExecutionNode(spreadJob, childs);
            addSpreadJobChilds(pipeline, job, chainsFrom, childs, idx);
            graph.add(node);
        }
    }


    private static void addSpreadChild(Pipeline pipeline, Job job, List<ExecutionNode> childs, Job chainJob, int idx) {
        Job childJob = getChildJob(pipeline, job, chainJob, idx);
        List<ExecutionNode> insideChilds = new LinkedList<>();
        childs.add(new ExecutionNode(childJob, insideChilds));
        addSpreadJobChilds(pipeline, chainJob, job.getChainsFrom(), insideChilds, idx);
    }

    private static Job getChildJob(Pipeline pipeline, Job job, Job chainJob, int idx) {
        Collection<String> inputs = chainJob.getSpread().getInputs();
        Map<String, List<String>> ins = new HashMap<>();
        for (String inputName : inputs) {
            Input input = chainJob.getInputById(inputName);
            if (input.getOriginJob().getId().equals(job.getId())) {
                Output output = job.getOutputById(input.getChainOutput());
                if (output.getType().equalsIgnoreCase(input.getType()) || usedBySameType(job, output, input.getType())) {
                    ins.put(inputName, new LinkedList<>());
                    ins.get(inputName).addAll(getValues(output.getValue().toString()));
                }
            } else {
                ins.put(inputName, new LinkedList<>());
                ins.get(inputName).addAll(getValues(input.getValue()));
            }
        }
        Job childJob = getSpreadJob(chainJob, ins, idx, pipeline);
        childJob.setParents(chainJob.getChainsFrom().stream().map(Job::getId).collect(Collectors.toList()));
        updateParentToSpreadParent(job, idx, childJob);
        return childJob;
    }

    private static void updateParentToSpreadParent(Job job, int idx, Job childJob) {
        List<String> parentsUpdated = childJob.getParents().stream().map((parent) -> {
            if (parent.equals(job.getId())) {
                return parent + "_" + parent + idx;
            } else {
                return parent;
            }
        }).collect(Collectors.toList());
        childJob.setParents(parentsUpdated);
    }

}
