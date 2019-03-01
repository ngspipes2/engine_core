package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.entities.factories.JobFactory;
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
                                       BiFunction<String, Job, List<String>> getOutValues, String fileSeparator) throws EngineException {

        List<Job> chainsFrom = job.getChainsFrom();
        List<Job> toRemove = new LinkedList<>();
        List<Job> spreadJobs = getExpandedJobs(pipeline, job, toRemove, getOutValues, fileSeparator);
        pipeline.addJobs(spreadJobs);
        pipeline.removeJobs(toRemove);
        addSpreadNodes(pipeline, job, graph, chainsFrom, spreadJobs, fileSeparator);
    }

    public static boolean isResourceType(String type) {
        boolean isFile = type.contains("ile");
        boolean directory = type.contains("irectory");
        return isFile || directory;
    }

    public static List<Job> getExpandedJobs(Pipeline pipeline, SimpleJob job, List<Job> toRemove,
                                            BiFunction<String, Job, List<String>> getOutValues,
                                            String fileSeparator) throws EngineException {
        List<Job> spreadJobs = new LinkedList<>();
        addSpreadJobs(job, spreadJobs, pipeline, getOutValues, fileSeparator);
        List<Job> spreadChildJobs = getSpreadChilds(job, pipeline);
        toRemove.add(job);
        toRemove.addAll(spreadChildJobs);

        for (int idx = 0; idx < spreadChildJobs.size(); idx++) {
            Job spreadChild = spreadChildJobs.get(idx);
            List<Job> expandedChildJobs = new LinkedList<>();
            addSpreadJobs(spreadChild, expandedChildJobs, pipeline, SpreadJobExpander::getMiddleOutputValues, fileSeparator);
            for (int index = 0; index < expandedChildJobs.size(); index++) {
                updateSpreadInputsChain(expandedChildJobs.get(index), spreadChildJobs.get(idx),job, spreadJobs.get(index));
            }
            spreadJobs.addAll(expandedChildJobs);
        }
        return spreadJobs;
    }

    private static void updateSpreadInputsChain(Job job, Job baseJob, Job parent, Job parentExpanded) {
        for (String inputToSpread : baseJob.getSpread().getInputs()) {
            Input input = job.getInputById(inputToSpread);
            if (input.getOriginJob().equals(parent))
                input.setOriginJob(parentExpanded);
        }
    }

    private static List<String> getMiddleOutputValues(String outputName, Job originJob) {
        String value = originJob.getOutputById(outputName).getValue().toString();
        List<String> values = new LinkedList<>();
        values.add(value);
        return values;
    }

    private static List<Job> getSpreadChilds(SimpleJob job, Pipeline pipeline) {
        List<Job> spreadChilds = new LinkedList<>();
        for (Job currJob : pipeline.getJobs()) {
            if (currJob.equals(job))
                continue;
            List<Input> chainInputs = getChainInputs(currJob);
            List<Input> spreadChildInputs = getSpreadChildInputs(chainInputs, job);
            if (!spreadChildInputs.isEmpty()) {
                if (!isJoinJob(job, currJob, spreadChildInputs)) {
                    spreadChilds.add(currJob);
                } else {
                    currJob.setInconclusive(true);
                }
            }
        }
        return spreadChilds;
    }
    private static boolean isJoinJob(Job parent, Job job, List<Input> chainInputs) {

        if (job.getSpread() == null) {
            return true;
        }

        boolean join = true;
        for (Input chainInput : chainInputs) {
            String inType = chainInput.getType();
            Output output = parent.getOutputById(chainInput.getChainOutput());
            String outType = output.getType();
            if (inType.equalsIgnoreCase(outType)) {
                join = false;
            } else {
                return true;
            }
        }

        return join;
    }

    private static List<Input> getSpreadChildInputs(List<Input> chainInputs, SimpleJob job) {
        List<Input> spreadChildInputs = new LinkedList<>();
        for (Input chainInput : chainInputs) {
            if (chainInput.getOriginStep().equals(job.getId()))
                spreadChildInputs.add(chainInput);
        }
        return spreadChildInputs;
    }

    private static List<Input> getChainInputs(Job job) {
        List<Input> chainInputs = new LinkedList<>();

        for (Input input: job.getInputs()) {
            if (!input.getOriginStep().isEmpty() && !input.getOriginStep().equals(job.getId())) {
                chainInputs.add(input);
            }
        }

        return chainInputs;
    }

    public static void addSpreadJobChilds(Pipeline pipeline, Job job, List<Job> chainsFrom,
                                          List<ExecutionNode> childs, int idx, String fileSeparator) {
        for (Job chainJob : chainsFrom) {
            if (chainJob.getSpread() != null) {
                addSpreadChild(pipeline, job, childs, chainJob, idx, fileSeparator);
            } else {
                chainJob.setInconclusive(!chainJob.isInconclusive());
            }
        }
    }

    public static void addSpreadJobs(Job job, List<Job> jobs, Pipeline pipeline,
                                     BiFunction<String, Job, List<String>> getOutValues,
                                     String fileSeparator) throws EngineException {
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

        spreadByInputs(job, jobs, pipeline, fileSeparator, valuesOfInputsToSpread);
    }

    private static void spreadByInputs(Job job, List<Job> jobs, Pipeline pipeline, String fileSeparator, Map<String, List<String>> valuesOfInputsToSpread) {
        int idx = 0;
        int len = getInputValuesLength(valuesOfInputsToSpread);

        while (idx < len) {
            Job jobToSpread = getSpreadJob(job, valuesOfInputsToSpread, idx, pipeline, fileSeparator);
            jobs.add(jobToSpread);
            idx++;
        }
    }

    public static List<String> getValues(String inputValue) {
        String suffix = "";
        int suffixIdx = inputValue.indexOf("]") - 1;
        inputValue = inputValue.replace("[", "");
        inputValue = inputValue.replace("]", "");

        if (suffixIdx != -1) {
            suffix = inputValue.length() == suffixIdx ? "" : inputValue.substring(suffixIdx);
        }

        String[] split = inputValue.split(",");
        List<String> inputsValues = new LinkedList<>();

        for (String str : split) {
            String value = str.trim();
            if (!value.contains(suffix))
                value = value + suffix;
            inputsValues.add(value);
        }

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

    private static List<Output> getCopyOfJobOutputs(List<Output> outputs, SimpleJob job, int idx) {
        List<Output> outs = new LinkedList<>();
        for (Output output : outputs) {
//            estou a ignorar que pode vir .* no fim [].*
            String value = getValues(output.getValue().toString()).get(idx);
            Output out = new Output(output.getName(), job, output.getType(), value);
            out.setUsedBy(output.getUsedBy());
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

    private static Environment copyEnvironment(Job job, String stepId, String workingDirectory, String fileSeparator) {
        Environment environment = new Environment();

        Environment baseEnvironment = job.getEnvironment();
        if (baseEnvironment == null)
            baseEnvironment = JobFactory.getJobEnvironment(job.getId(), workingDirectory, fileSeparator);
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
        if (job.getChainsFrom().isEmpty())
            return false;

        for (Input in : job.getInputs()) {
            String originStep = in.getOriginStep();
            if (originStep != null && !originStep.equals(job.getId())) {
//                Job chainJob = pipeline.getJobById(originStep);
//                if (chainJob.getSpread() != null ^ job.getSpread() != null)
                    return true;
            }
        }
        return false;
    }

    private static SimpleJob getSpreadJob(Job job, Map<String, List<String>> inputs, int idx,
                                          Pipeline pipeline, String fileSeparator) {
        if (job instanceof SimpleJob) {
            SimpleJob simpleJob = (SimpleJob) job;
            ExecutionContext executionContext = simpleJob.getExecutionContext();

            String id = job.getId() + "_" + job.getId() + idx;
            Environment env = copyEnvironment(job, id, pipeline.getEnvironment().getWorkDirectory(), fileSeparator);
            SimpleJob sJob = new SimpleJob(id, env, simpleJob.getCommand(), executionContext);
            List<Input> jobInputs = getCopyOfJobInputs(job.getInputs(), inputs, idx, sJob, pipeline, job.getId());
            sJob.setInputs(jobInputs);
            List<Output> jobOutputs = getCopyOfJobOutputs(job.getOutputs(), simpleJob, idx);
            sJob.setOutputs(jobOutputs);
            simpleJob.getChainsFrom().forEach(sJob::addChainsFrom);
            return sJob;
        }
        //falta compose jobs
        throw new NotImplementedException();
    }

    private static void addSpreadNodes(Pipeline pipeline, SimpleJob job, Collection<ExecutionNode> graph,
                                       List<Job> chainsFrom, List<Job> spreadJobs, String fileSeparator) {
        for (int idx = 0; idx < spreadJobs.size(); idx++) {
            Job spreadJob = spreadJobs.get(idx);
            List<ExecutionNode> childs = new LinkedList<>();
//            spreadJob.setParents(job.getChainsFrom().stream().map(Job::getId).collect(Collectors.toList()));
            ExecutionNode node = new ExecutionNode(spreadJob, childs);
            addSpreadJobChilds(pipeline, job, chainsFrom, childs, idx, fileSeparator);
            graph.add(node);
        }
    }


    public static Job getChildJob(Pipeline pipeline, Job job, Job chainJob, int idx, String fileSeparator) {
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
        Job childJob = getSpreadJob(chainJob, ins, idx, pipeline, fileSeparator);
        updateChains(job, idx, childJob, pipeline);
        if (!childJob.getParents().isEmpty())
            updateParentToSpreadParent(job, idx, childJob);
        return childJob;
    }

    private static void addSpreadChild(Pipeline pipeline, Job job, List<ExecutionNode> childs,
                                       Job chainJob, int idx, String fileSeparator) {
        Job childJob = getChildJob(pipeline, job, chainJob, idx, fileSeparator);
        List<ExecutionNode> insideChilds = new LinkedList<>();
        childs.add(new ExecutionNode(childJob, insideChilds));
        addSpreadJobChilds(pipeline, chainJob, job.getChainsFrom(), insideChilds, idx, fileSeparator);
    }

    private static void updateChains(Job job, int idx, Job childJob, Pipeline pipeline) {
        if (job.getSpread() != null && childJob.getSpread() != null) {
            String newChainToId = job.getId() + "_" + job.getId() + idx;
            childJob.getChainsTo().remove(job);
            childJob.addChainsTo(pipeline.getJobById(newChainToId));
        }
    }

    private static void updateParentToSpreadParent(Job job, int idx, Job childJob) {
        List<String> parentsUpdated = childJob.getParents().stream().map((parent) -> {
            if (parent.equals(job.getId())) {
                return parent + "_" + parent + idx;
            } else {
                return parent;
            }
        }).collect(Collectors.toList());
    }

}
