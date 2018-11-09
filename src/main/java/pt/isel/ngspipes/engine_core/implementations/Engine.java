package pt.isel.ngspipes.engine_core.implementations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.JobFactory;
import pt.isel.ngspipes.engine_core.utils.SpreadCombiner;
import pt.isel.ngspipes.engine_core.utils.TopologicSorter;
import pt.isel.ngspipes.engine_core.utils.ValidateUtils;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;

public abstract class Engine implements IEngine {

    Logger logger;
    String workingDirectory;
    final Map<String, Pipeline> pipelines = new HashMap<>();

    Engine(String workingDirectory) {
        this.workingDirectory = workingDirectory;
        logger = LogManager.getLogger(Engine.class.getName());
    }

    @Override
    public Pipeline execute(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                            Arguments arguments) throws EngineException {
        String id = generateExecutionId(pipelineDescriptor.getName());
//        validate(pipelineDescriptor, parameters);
        Pipeline pipeline = createPipelineContext(pipelineDescriptor, parameters, arguments, id);
        return internalExecute(arguments.parallel, pipeline);
    }

    @Override
    public Pipeline execute(String intermediateRepresentation, Arguments arguments) throws EngineException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        try {
            Pipeline pipeline = mapper.readValue(intermediateRepresentation, Pipeline.class);
            pipeline.setEnvironment(JobFactory.getJobEnvironment(pipeline.getName(), workingDirectory));
            return internalExecute(arguments.parallel, pipeline);
        } catch (IOException e) {
            throw new EngineException("Error loading pipeline from intermediate representation supplied", e);
        }
    }


    protected abstract void stage(Pipeline pipeline, List<ExecutionBlock> executionBlocks) throws EngineException;
    abstract List<String> getOutputValuessFromJob(String chainOutput, Job originJob);
    abstract List<String> getOutputValuesFromMultipleJobs(String chainOutput, Job originJob);

    protected void addSpreadNodes(Pipeline pipeline, ExecutionBlock block) throws EngineException {

        List<ExecutionNode> newSpreadNodes = new LinkedList<>();
        List<ExecutionNode> toDeleteSpreadFromExecBlock = new LinkedList<>();
        List<ExecutionNode> jobsToExecute = block.getJobsToExecute();
        List<Job> jobs = new LinkedList<>();
        for (ExecutionNode node : jobsToExecute) {
            toDeleteSpreadFromExecBlock.add(node);
            Job job = node.getJob();
            if(job.getSpread() != null) {
                addSpreadNodes(node, newSpreadNodes, new LinkedList<>(), jobs, pipeline, this::getMiddleInputValues);
            } else {
                updateJoinJobChainInputValues(job);
            }
        }

        jobsToExecute.removeAll(toDeleteSpreadFromExecBlock);
        jobsToExecute.addAll(newSpreadNodes);
        block.getJobs().addAll(jobs);
    }

    private void updateJoinJobChainInputValues(Job job) throws EngineException {
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

    private Pipeline internalExecute(boolean parallel, Pipeline pipeline) {
        initState(pipeline);
        registerPipeline(pipeline);
        executePipeline(parallel, pipeline);
        return pipeline;
    }

    private Pipeline createPipelineContext(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                                           Arguments arguments, String id) throws EngineException {
        String pipelineWorkingDirectory = getPipelineWorkingDirectory(id);
        return JobFactory.create(id, pipelineDescriptor, parameters,
        arguments, pipelineWorkingDirectory);
    }

    private void registerPipeline(Pipeline pipeline) {
        pipelines.put(pipeline.getName(), pipeline);
    }

    private void initState(Pipeline pipeline) {
        ExecutionState state = new ExecutionState();
        state.setState(StateEnum.STAGING);
        pipeline.setState(state);
    }

    private void executePipeline(boolean parallel, Pipeline pipeline) {
        TaskFactory.createAndExecuteTask(() -> {
            try {
                execute(pipeline, parallel);
            } catch (EngineException e) {
                ExecutionState executionState = new ExecutionState(StateEnum.FAILED, e);
                pipeline.setState(executionState);
                logger.error("Pipeline " + pipeline.getName(), e);
            }
        });
    }

    private void execute(Pipeline pipeline, boolean parallel) throws EngineException {
        Collection<ExecutionNode> executionGraph = topologicalSort(pipeline, parallel);
        pipeline.setGraph(executionGraph);
        List<ExecutionBlock> executionBlocks = getPipelineExecutionBlocks(pipeline);
        stage(pipeline, executionBlocks);
    }

    private List<ExecutionBlock> getPipelineExecutionBlocks(Pipeline pipeline) throws EngineException {
        List<ExecutionBlock> blocks = new LinkedList<>();
        List<Job> spreadJobs = new LinkedList<>();

        pipeline.getJobs().forEach((j) -> {
            if (j.getSpread() != null) {
                spreadJobs.add(j);
            }
        });
        if (spreadJobs.size() == 0) {
            blocks.add(new ExecutionBlock((List<ExecutionNode>) pipeline.getGraph(), pipeline.getJobs()));
        } else {
            List<String> addedNodes = new LinkedList<>();
            List<Job> execBlockJobs = new LinkedList<>();
            ExecutionBlock block;
            List<ExecutionNode> nodes = new LinkedList<>();
            for (ExecutionNode node : pipeline.getGraph()) {
                    if (node.getJob().getSpread() == null) {
                        ExecutionNode parentNode = new ExecutionNode(node.getJob());
                        nodes.add(parentNode);
                        addedNodes.add(parentNode.getJob().getId());
                        addNoSpreadChilds(parentNode, node, addedNodes, pipeline, execBlockJobs);
                    } else {
                        addSpreadNodes(node, nodes, addedNodes, execBlockJobs, pipeline, this::getInitInputValues);
                    }
            }
            block = new ExecutionBlock(nodes, execBlockJobs);
            blocks.add(block);

            List<ExecutionNode> currentNodes = new LinkedList<>(pipeline.getGraph());
            while (addedNodes.size() != pipeline.getJobs().size()) {
                getRestOfExecutionBlocks(blocks, addedNodes, currentNodes, pipeline);
            }
        }
        return blocks;
    }

    private void addNoSpreadChilds(ExecutionNode parentNode, ExecutionNode node, List<String> addedNodes,
                                   Pipeline pipeline, List<Job> execBlockJobs) throws EngineException {
        for (ExecutionNode child : node.getChilds()) {
            Job job = child.getJob();
            if (job.getSpread() == null) {
                ExecutionNode childNode = new ExecutionNode(job);
                parentNode.getChilds().add(childNode);
                addedNodes.add(childNode.getJob().getId());
                execBlockJobs.add(job);
                addNoSpreadChilds(childNode, child, addedNodes, pipeline, execBlockJobs);
            } else if(!hasChainInput(job)) {
                addSpreadNodes(child, parentNode.getChilds(), addedNodes, execBlockJobs, pipeline, this::getInitInputValues);
            }
        }
    }

    private void getRestOfExecutionBlocks(List<ExecutionBlock> blocks, List<String> addedNodes,
                                          List<ExecutionNode> currNodes, Pipeline pipeline) throws EngineException {
        List<ExecutionNode> nodes = new LinkedList<>();
        ExecutionBlock block;
        List<ExecutionNode> nextNodes = new LinkedList<>();
        List<ExecutionNode> toRemove = new LinkedList<>();
        List<Job> jobs = new LinkedList<>();
        for (ExecutionNode node : currNodes) {
            for (ExecutionNode child : node.getChilds()) {
                Job job = child.getJob();
                if (!addedNodes.contains(job.getId())) {
                    jobs.add(job);
                    ExecutionNode parentNode = new ExecutionNode(job);
                    nodes.add(parentNode);
                    addedNodes.add(parentNode.getJob().getId());
                   if (job.getSpread() == null) {
                       addNoSpreadChilds(parentNode, child, addedNodes, pipeline, jobs);
                   }
               }
               nextNodes.add(child);
            }
            toRemove.add(node);
        }

        if (!nodes.isEmpty()) {
            block = new ExecutionBlock(nodes, jobs);
            blocks.add(block);
        }

        for (ExecutionNode node : toRemove) {
            currNodes.remove(node);
        }
        currNodes.addAll(nextNodes);
    }

    private boolean hasChainInput(Job job) {
        for (Input input : job.getInputs()) {
            if (input.getOriginStep() == null)
                continue;
            if (!input.getOriginStep().equals(job.getId()))
                return true;
        }

        return false;
    }

    private void addSpreadNodes(ExecutionNode node, List<ExecutionNode> nodes, List<String> addedNodes, List<Job> jobs,
                                Pipeline pipeline, BiFunction<Input, Pipeline, List<String>> values) throws EngineException {
        Job job = node.getJob();
        addedNodes.add(job.getId());
        Spread spread = job.getSpread();
        Map<String, List<String>> valuesOfInputsToSpread = getInputValuesToSpread(job, pipeline, values);
        if (spread.getStrategy() != null)
            SpreadCombiner.getInputsCombination(spread.getStrategy(), valuesOfInputsToSpread);

        int idx = 0;
        int len = getInputValuesLength(valuesOfInputsToSpread);

        while (idx < len) {
            Job jobToSpread = getSpreadJob(job, valuesOfInputsToSpread, idx, pipeline);
            jobToSpread.setParents(job.getParents());
            jobs.add(jobToSpread);
            ExecutionNode execNode = new ExecutionNode(jobToSpread);
            nodes.add(execNode);
            idx++;
        }
    }

    private SimpleJob getSpreadJob(Job job, Map<String, List<String>> inputs, int idx, Pipeline pipeline) {
        if (job instanceof SimpleJob) {
            SimpleJob simpleJob = (SimpleJob) job;
            ExecutionContext executionContext = simpleJob.getExecutionContext();

            String id = job.getId() + "_" + job.getId() + idx;
            Environment env = copyEnvironment(job, id, pipeline.getEnvironment().getWorkDirectory());
            SimpleJob sJob = new SimpleJob(id, env, simpleJob.getCommand(), executionContext);
            List<Input> jobInputs = getCopyOfJobInputs(job.getInputs(), inputs, idx, simpleJob, pipeline);
            simpleJob.setInputs(jobInputs);
            List<Output> jobOutputs = getCopyOfJobOutputs(job.getOutputs(), simpleJob);
            simpleJob.setOutputs(jobOutputs);
            sJob.setParents(job.getParents());
            return sJob;
        }
        //falta compose jobs
        throw new NotImplementedException();
    }

    private List<Output> getCopyOfJobOutputs(List<Output> outputs, SimpleJob job) {
        List<Output> outs = new LinkedList<>();
        for (Output output : outputs) {
            Output out = new Output(output.getName(), job, output.getType(), output.getValue());
            outs.add(out);
        }
        return outs;
    }

    private List<Input> getCopyOfJobInputs(List<Input> jobInputs, Map<String, List<String>> inputs,
                                           int idx, Job job, Pipeline pipeline) {
        List<Input> copiedInputs = new LinkedList<>();
        for (Input input : jobInputs) {
            List<Input> subInputs = input.getSubInputs();
            String originStep = input.getOriginStep();
            Job originJob = originStep != null ? originStep.equals(job.getId()) ? job : pipeline.getJobById(originStep) : job;
            List<Input> copyOfJobInputs = subInputs == null ? new LinkedList<>() : getCopyOfJobInputs(subInputs, inputs, idx, job, pipeline);
            copiedInputs.add(new Input(input.getName(), originJob, input.getChainOutput(),
                                        input.getType(), inputs.get(input.getName()).get(idx), input.getPrefix(),
                                        input.getSeparator(), input.getSuffix(), copyOfJobInputs));
        }
        return copiedInputs;
    }

    private Environment copyEnvironment(Job job, String stepId, String workingDirectory) {
        Environment environment = new Environment();

        Environment baseEnvironment = job.getEnvironment();
        if (baseEnvironment == null)
            baseEnvironment = JobFactory.getJobEnvironment(job.getId(), workingDirectory);
        environment.setDisk(baseEnvironment.getDisk());
        environment.setMemory(baseEnvironment.getMemory());
        environment.setCpu(baseEnvironment.getCpu());
        environment.setWorkDirectory(baseEnvironment.getWorkDirectory() + File.separatorChar + stepId);

        return environment;
    }

    private int getInputValuesLength(Map<String, List<String>> valuesOfInputsToSpread) {
        if (valuesOfInputsToSpread.values().iterator().hasNext())
            return valuesOfInputsToSpread.values().iterator().next().size();
        return 0;
    }

    private Map<String, List<String>> getInputValuesToSpread(Job job, Pipeline pipeline,
                                                             BiFunction<Input, Pipeline, List<String>> getValues) {
        Map<String, List<String>> valuesOfInputsToSpread = new HashMap<>();
        List<String> inputsToSpread = (List<String>) job.getSpread().getInputs();

        for (Input input : job.getInputs()) {
            if (inputsToSpread.contains(input.getName())) {
                valuesOfInputsToSpread.put(input.getName(), getValues.apply(input, pipeline));
            }
        }

        return valuesOfInputsToSpread;
    }

    private List<String> getMiddleInputValues(Input input, Pipeline pipeline) {
        String chainOutput = input.getChainOutput();
        if (chainOutput != null && !chainOutput.isEmpty()) {
            Job originJob = input.getOriginJob()  == null ? pipeline.getJobById(chainOutput) : input.getOriginJob();
            if (originJob.getSpread() != null) {
                return getOutputValuesFromMultipleJobs(chainOutput, originJob);
            } else {
                return getOutputValuessFromJob(chainOutput, originJob);
            }
        } else
            return getInitInputValues(input, pipeline);
    }

    private List<String> getInitInputValues(Input input, Pipeline pipeline) {
        String inputValue = input.getValue();
        inputValue = inputValue.replace("[", "");
        inputValue = inputValue.replace("]", "");

        String[] split = inputValue.split(",");
        List<String> inputsValues = new LinkedList<>();

        for (String str : split)
            inputsValues.add(str.trim());

        return inputsValues;
    }

    private String generateExecutionId(String pipelineName) {
        String pipelineNameID = pipelineName == null || pipelineName.isEmpty() ? "" : pipelineName;
        String currTime = Calendar.getInstance().getTimeInMillis() + "";
        return pipelineNameID + "_" + currTime;
    }


    private Collection<ExecutionNode> topologicalSort(Pipeline pipeline, boolean parallel) {
        return parallel ? TopologicSorter.parallelSort(pipeline) : TopologicSorter.sequentialSort(pipeline);
    }

    private void validate(IPipelineDescriptor pipelineDescriptor,
                          Map<String, Object> parameters) throws EngineException {
        ValidateUtils.validateRepositories(pipelineDescriptor.getRepositories());
        ValidateUtils.validateOutputs(pipelineDescriptor, parameters);
        ValidateUtils.validateSteps(pipelineDescriptor, parameters);
        ValidateUtils.validateNonCyclePipeline(pipelineDescriptor, parameters);
    }

    private String getPipelineWorkingDirectory(String executionId) {
        String workDirectory = this.workingDirectory + File.separatorChar + executionId;
        return workDirectory.replace(" ", "");
    }

}
