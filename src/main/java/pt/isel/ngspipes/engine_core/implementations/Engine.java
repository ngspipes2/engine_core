package pt.isel.ngspipes.engine_core.implementations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.StateEnum;
import pt.isel.ngspipes.engine_core.entities.contexts.Job;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_core.entities.factories.JobFactory;
import pt.isel.ngspipes.engine_core.entities.factories.PipelineFactory;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.JacksonUtils;
import pt.isel.ngspipes.engine_core.utils.SpreadJobExpander;
import pt.isel.ngspipes.engine_core.utils.TopologicSorter;
import pt.isel.ngspipes.engine_core.utils.ValidateUtils;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

public abstract class Engine implements IEngine {

    Logger logger;
    String workingDirectory;
    private final String name;
    final Map<String, Pipeline> pipelines = new HashMap<>();
    final String fileSeparator;

    Engine(String workingDirectory, String name, String fileSeparator) {
        this.workingDirectory = workingDirectory;
        this.name = name;
        logger = LogManager.getLogger(name);

        this.fileSeparator = fileSeparator;
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
        Pipeline pipeline;
        try {
            pipeline = JacksonUtils.deserialize(intermediateRepresentation, Pipeline.class);
            pipeline.setEnvironment(JobFactory.getJobEnvironment(pipeline.getName(), workingDirectory, fileSeparator));
        } catch (IOException e) {
            logger.error("Error loading pipeline from intermediate representation supplied", e);
            throw new EngineException("Error loading pipeline from intermediate representation supplied", e);
        }
        return internalExecute(arguments.parallel, pipeline);
    }



    void runInconclusiveDependencies(SimpleJob job, Pipeline pipeline, Consumer<String> wait, String id) {
        if (job.isInconclusive()) {
            job.setInconclusive(false);
            TaskFactory.createAndExecuteTask(() -> {
                wait.accept(id);
                Collection<ExecutionNode> graph = TopologicSorter.parallelSort(pipeline, job);
                try {
                    for (ExecutionNode node : graph) {
                        SimpleJob childJob = (SimpleJob) node.getJob();
                        if (childJob.getSpread() != null) {
                                SpreadJobExpander.expandSpreadJob(pipeline, childJob, graph, this::getOutputValues, fileSeparator);
                        }
                    }
                    run(pipeline, graph);
                } catch (EngineException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    void updateState(Pipeline pipeline, Job job, EngineException e, StateEnum state) {
        ExecutionState newState = new ExecutionState(state, e);
        job.getState().setState(newState.getState());
        if (e != null)
            pipeline.setState(newState);
    }

    protected abstract void configure(Pipeline pipeline) throws EngineException;
    protected abstract void run(Pipeline pipeline, Collection<ExecutionNode> graph) throws EngineException;
    protected abstract void copyPipelineInputs(Pipeline pipeline) throws EngineException;
    abstract List<String> getOutputValuesFromJob(String chainOutput, Job originJob);
    abstract List<String> getOutputValuesFromSpreadJob(String chainOutput, Job originJob, String spreadId);

    List<String> getOutputValues(String outputName, Job job) {
        if (job.getSpread() != null) {
            return getOutputValuesFromSpreadJob(outputName, job, "");
        } else {
            return getOutputValuesFromJob(outputName, job);
        }
    }

    private void stage(Pipeline pipeline) throws EngineException {
        ValidateUtils.validateJobs(pipeline.getJobs());
        configure(pipeline);
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
        return PipelineFactory.create(id, pipelineDescriptor, parameters, arguments,
                                        pipelineWorkingDirectory, fileSeparator);
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
                logger.error("Error executing pipeline: " + pipeline.getName(), e);
                ExecutionState executionState = new ExecutionState(StateEnum.FAILED, e);
                pipeline.setState(executionState);
            }
        });
    }

    private void execute(Pipeline pipeline, boolean parallel) throws EngineException {
        Collection<ExecutionNode> executionGraph = topologicalSort(pipeline, parallel);
        pipeline.setGraph(executionGraph);
        stage(pipeline);
    }

    private String generateExecutionId(String pipelineName) {
        String pipelineNameID = pipelineName == null || pipelineName.isEmpty() ? "" : pipelineName;
        String currTime = Calendar.getInstance().getTimeInMillis() + "";
        return pipelineNameID + "_" + currTime;
    }


    private Collection<ExecutionNode> topologicalSort(Pipeline pipeline, boolean parallel) {
        if (isSpreadPipeline(pipeline))
            return TopologicSorter.parallelSort(pipeline);
        return parallel ? TopologicSorter.parallelSort(pipeline) : TopologicSorter.sequentialSort(pipeline);
    }

    private boolean isSpreadPipeline(Pipeline pipeline) {
        for (Job job : pipeline.getJobs())
            if (job.getSpread() != null)
                return true;
        return false;
    }

    private void validate(IPipelineDescriptor pipelineDescriptor,
                          Map<String, Object> parameters) throws EngineException {
        ValidateUtils.validateRepositories(pipelineDescriptor.getRepositories());
        ValidateUtils.validateOutputs(pipelineDescriptor, parameters);
        ValidateUtils.validateNonCyclePipeline(pipelineDescriptor, parameters);
    }

    private String getPipelineWorkingDirectory(String executionId) {
        String workDirectory = this.workingDirectory + fileSeparator + executionId;
        return workDirectory.replace(" ", "");
    }

}
