package pt.isel.ngspipes.engine_core.implementations;

import com.github.brunomndantas.tpl4j.factory.TaskFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.isel.ngspipes.engine_common.entities.Arguments;
import pt.isel.ngspipes.engine_common.entities.ExecutionNode;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Output;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.entities.factory.JobFactory;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;
import pt.isel.ngspipes.engine_common.interfaces.IExecutor;
import pt.isel.ngspipes.engine_common.utils.IOUtils;
import pt.isel.ngspipes.engine_common.utils.JacksonUtils;
import pt.isel.ngspipes.engine_common.utils.TopologicSorter;
import pt.isel.ngspipes.engine_common.utils.ValidateUtils;
import pt.isel.ngspipes.engine_core.entities.PipelineFactory;
import pt.isel.ngspipes.engine_core.entities.Status;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Engine implements IEngine {

    Logger logger = LogManager.getLogger(Engine.class.getName());
    private final IExecutor executor;
    final Map<String, Pipeline> pipelines = new HashMap<>();

    public Engine(IExecutor executor) {
        this.executor = executor;
    }

    @Override
    public Pipeline execute(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                            Arguments arguments) throws EngineException {
        String id = generateExecutionId(pipelineDescriptor.getName());//"1234";
        validate(pipelineDescriptor, parameters);
        Pipeline pipeline = createPipeline(pipelineDescriptor, parameters, arguments, id, executor.getFileSeparator());
        return internalExecute(arguments.parallel, pipeline);
    }

    @Override
    public Pipeline execute(String intermediateRepresentation, Arguments arguments) throws EngineException {
        Pipeline pipeline;
        try {
            pipeline = JacksonUtils.deserialize(intermediateRepresentation, Pipeline.class);
            pipeline.setEnvironment(JobFactory.getJobEnvironment(pipeline.getName(), executor.getWorkingDirectory(), executor.getFileSeparator()));
        } catch (IOException e) {
            logger.error("Error loading pipeline from intermediate representation supplied", e);
            throw new EngineException("Error loading pipeline from intermediate representation supplied", e);
        }
        return internalExecute(arguments.parallel, pipeline);
    }

    @Override
    public boolean stop(String executionId) throws EngineException {
        try {
            return executor.stop(executionId);
        } catch (ExecutorException e) {
            throw new EngineException("Error stopping pipeline " + executionId + " execution.", e);
        }
    }

    @Override
    public boolean clean(String executionId) throws EngineException {
         try {
            return executor.clean(executionId);
        } catch (ExecutorException e) {
            throw new EngineException("Error cleaning pipeline " + executionId + " execution.", e);
        }
    }

    @Override
    public boolean cleanAll() throws EngineException {
        try {
            return executor.cleanAll();
        } catch (ExecutorException e) {
            throw new EngineException("Error cleaning pipelines.", e);
        }
    }

    @Override
    public void getPipelineOutputs(String executionId, String outputDirectory) throws EngineException {
        try {
            Pipeline pipelineExecId = pipelines.get(executionId);
            executor.getPipelineOutputs(pipelineExecId, outputDirectory);
        } catch (ExecutorException e) {
            throw new EngineException("Error getting pipeline " + executionId + "outputs.", e);
        }
    }

    @Override
    public Status getStatus(String executionId) {
        if (pipelines.containsKey(executionId)) {
            Pipeline pipelineExecId = pipelines.get(executionId);
            Map<String, ExecutionState> jobsStatus = new HashMap<>();
            for (Job job : pipelineExecId.getJobs())
                jobsStatus.put(job.getId(), job.getState());
            return new Status(pipelineExecId.getState(), jobsStatus);
        }
        return null;
    }


    private void stage(Pipeline pipeline) throws EngineException {
        try {
            ValidateUtils.validateJobs(pipeline.getJobs());
        } catch (EngineCommonException e) {
            throw new EngineException("Error validating pipeline jobs", e);
        }
        pipeline.getState().setState(StateEnum.SCHEDULE);
        schedulePipeline(pipeline);
    }

    private void schedulePipeline(Pipeline pipeline) throws EngineException {
        logger.trace("Engine :: Scheduling pipeline " + pipeline.getName() + " " + pipeline.getName());
        Collection<ExecutionNode> executionGraph = pipeline.getGraph();
        try {
            executor.execute(pipeline);
        } catch (ExecutorException e) {
            throw new EngineException("Error executing pipeline " + pipeline.getName(), e);
        }
    }

    private Pipeline internalExecute(boolean parallel, Pipeline pipeline) {
        initState(pipeline);
        registerPipeline(pipeline);
        executePipeline(parallel, pipeline);
        return pipeline;
    }

    private Pipeline createPipeline(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                                    Arguments arguments, String id, String fileSeparator) throws EngineException {
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
        TaskFactory.createAndStart("executor" + pipeline.getName(), () -> {
            try {
                execute(pipeline, parallel);
            } catch (EngineException e) {
                logger.error("Error executing pipeline: " + pipeline.getName(), e);
                ExecutionState executionState = new ExecutionState(StateEnum.FAILED, e);
                pipeline.setState(executionState);
                throw e;
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
        try {
            ValidateUtils.validateRepositories(pipelineDescriptor.getRepositories());
            ValidateUtils.validateOutputs(pipelineDescriptor, parameters);
            ValidateUtils.validateNonCyclePipeline(pipelineDescriptor, parameters);
        } catch (EngineCommonException e) {
            throw new EngineException("Error validating pipeline " + pipelineDescriptor.getName() + " .", e);
        }
    }

    private String getPipelineWorkingDirectory(String executionId) {
        String workDirectory = executor.getWorkingDirectory() + executor.getFileSeparator() + executionId;
        return workDirectory.replace(" ", "");
    }

}
