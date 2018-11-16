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
    final String name;
    final Map<String, Pipeline> pipelines = new HashMap<>();

    Engine(String workingDirectory, String name) {
        this.workingDirectory = workingDirectory;
        this.name = name;
        logger = LogManager.getLogger(Engine.class.getName());
    }

    @Override
    public Pipeline execute(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                            Arguments arguments) throws EngineException {
        String id = generateExecutionId(pipelineDescriptor.getName());
        validate(pipelineDescriptor, parameters);
        Pipeline pipeline = createPipeline(pipelineDescriptor, parameters, arguments, id);
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


    protected abstract void run(Pipeline pipeline, Collection<ExecutionNode> graph) throws EngineException;
    protected abstract void copyPipelineInputs(Pipeline pipeline) throws EngineException;
    abstract List<String> getOutputValuesFromJob(String chainOutput, Job originJob);
    abstract List<String> getOutputValuesFromMultipleJobs(String chainOutput, Job originJob);


    void addSpreadNodes(Job job, List<Job> jobs, Pipeline pipeline) throws EngineException {
        Spread spread = job.getSpread();
        BiFunction<Input, Pipeline, List<String>> values;

        if (isSpreadChain(job, pipeline)) {
            values = this::getMiddleInputValues;
        } else {
            values = this::getInitInputValues;
        }

        Map<String, List<String>> valuesOfInputsToSpread = getInputValuesToSpread(job, pipeline, values);
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

    void addSpreadJobChilds(Pipeline pipeline, Job job, List<Job> chainsFrom, List<ExecutionNode> childs) {
        for (Job chainJob : chainsFrom) {
            if (chainJob.getSpread() != null) {
                addSpreadChild(pipeline, job, childs, chainJob);
            } else {
                chainJob.setInconclusive(!chainJob.isInconclusive());
            }
        }
    }

    List<Job> getChainsFrom(List<Job> jobs, Job fromJob) {
        List<Job> chainsFrom = new LinkedList<>();

        for (Job currJob : jobs) {
            if (currJob.getParents().contains(fromJob.getId())) {
                chainsFrom.add(currJob);
            }
        }

        return chainsFrom;
    }


    private void addSpreadChild(Pipeline pipeline, Job job, List<ExecutionNode> childs, Job chainJob) {
        Collection<String> inputs = chainJob.getSpread().getInputs();
        for (String inputName : inputs) {
            Input input = chainJob.getInputById(inputName);
            if (input.getOriginJob().getId().equals(job.getId())) {
                Output output = job.getOutputById(input.getChainOutput());
                if (output.getType().equalsIgnoreCase(input.getType()) || usedBySameType(job, output, input.getType())) {
                    Map<String, List<String>> ins = new HashMap<>();
                    ins.put(inputName, new LinkedList<>());
                    ins.get(inputName).add(output.getValue().toString());
                    Job childJob = getSpreadJob(chainJob, ins, 0, pipeline);
                    List<ExecutionNode> insideChilds = new LinkedList<>();
                    childs.add(new ExecutionNode(childJob, insideChilds));
                    addSpreadJobChilds(pipeline, chainJob, getChainsFrom(pipeline.getJobs(), chainJob), insideChilds);
                }
            }
        }
    }

    private boolean usedBySameType(Job job, Output output, String type) {
        if (output.getType().equalsIgnoreCase("string")) {
            List<String> usedBy = output.getUsedBy();
            for (String uses : usedBy) {
                if (job.getOutputById(uses).getType().equalsIgnoreCase(type))
                    return true;
            }
        }
        return false;
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

    private void stage(Pipeline pipeline) throws EngineException {
        ValidateUtils.validateJobs(pipeline.getJobs());
        copyPipelineInputs(pipeline);
        schedulePipeline(pipeline);
        pipeline.getState().setState(StateEnum.SCHEDULE);
    }

    private void schedulePipeline(Pipeline pipeline) throws EngineException {
        logger.trace(name + ":: Scheduling pipeline " + pipeline.getName() + " " + pipeline.getName());
        Collection<ExecutionNode> executionGraph = pipeline.getGraph();
        run(pipeline, executionGraph);
    }

    private Pipeline internalExecute(boolean parallel, Pipeline pipeline) {
        initState(pipeline);
        registerPipeline(pipeline);
        executePipeline(parallel, pipeline);
        return pipeline;
    }

    private Pipeline createPipeline(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
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
        stage(pipeline);
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

    private static boolean isSpreadChain(Job job, Pipeline pipeline) {
        for (Input in : job.getInputs()) {
            if (in.getOriginStep() != null && in.getOriginStep().equals(job.getId())) {
                if (pipeline.getJobById(in.getOriginStep()).getSpread() != null)
                    return true;
            }
        }
        return false;
    }

    private SimpleJob getSpreadJob(Job job, Map<String, List<String>> inputs, int idx, Pipeline pipeline) {
        if (job instanceof SimpleJob) {
            SimpleJob simpleJob = (SimpleJob) job;
            ExecutionContext executionContext = simpleJob.getExecutionContext();

            String id = job.getId() + "_" + job.getId() + idx;
            Environment env = copyEnvironment(job, id, pipeline.getEnvironment().getWorkDirectory());
            SimpleJob sJob = new SimpleJob(id, env, simpleJob.getCommand(), executionContext);
            sJob.setInconclusive(simpleJob.isInconclusive());
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
                return getOutputValuesFromJob(chainOutput, originJob);
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
        ValidateUtils.validateNonCyclePipeline(pipelineDescriptor, parameters);
    }

    private String getPipelineWorkingDirectory(String executionId) {
        String workDirectory = this.workingDirectory + File.separatorChar + executionId;
        return workDirectory.replace(" ", "");
    }

}
