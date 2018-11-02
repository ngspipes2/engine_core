package pt.isel.ngspipes.engine_core.implementations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.StateEnum;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.JobFactory;
import pt.isel.ngspipes.engine_core.utils.TopologicSorter;
import pt.isel.ngspipes.engine_core.utils.ValidateUtils;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.File;
import java.io.IOException;
import java.util.*;

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
            return internalExecute(arguments.parallel, pipeline);
        } catch (IOException e) {
            throw new EngineException("Error loading pipeline from intermediate representation supplied", e);
        }
    }


    protected abstract void stage(Pipeline pipeline) throws EngineException;


    private Pipeline internalExecute(boolean parallel, Pipeline pipeline) {
        initState(pipeline);
        registerPipeline(pipeline);
        executePipeline(parallel, pipeline);
        return pipeline;
    }

    private Pipeline createPipelineContext(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters, Arguments arguments, String id) throws EngineException {
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
