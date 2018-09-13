package pt.isel.ngspipes.engine_core.implementations;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.StateEnum;
import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.ContextFactory;
import pt.isel.ngspipes.engine_core.utils.TopologicSorter;
import pt.isel.ngspipes.engine_core.utils.ValidateUtils;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.File;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class Engine implements IEngine {

    Logger logger;
    String workingDirectory;
    final Map<String, PipelineContext> pipelines = new HashMap<>();

    Engine(String workingDirectory) {
        this.workingDirectory = workingDirectory;
        logger = LogManager.getLogger(Engine.class.getName());
    }

    @Override
    public PipelineContext execute(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                          Arguments arguments) throws EngineException {
        String id = generateExecutionId(pipelineDescriptor.getName());
        PipelineContext pipeline = createPipelineContext(pipelineDescriptor, parameters, arguments, id);
        initState(pipeline);
        registerPipeline(pipeline);
        executePipeline(arguments.parallel, pipeline);
        return pipeline;
    }


    protected abstract void stage(PipelineContext pipeline) throws EngineException;

    void executeSubPipeline(String stepId, PipelineContext pipeline) throws EngineException {
        PipelineContext subPipeline = ContextFactory.create(stepId, pipeline);
        execute(subPipeline, true);
    }


    private PipelineContext createPipelineContext(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters, Arguments arguments, String id) throws EngineException {
        String pipelineWorkingDirectory = getPipelineWorkingDirectory(id);
        return ContextFactory.create(id, pipelineDescriptor, parameters,
        arguments, pipelineWorkingDirectory);
    }

    private void registerPipeline(PipelineContext pipeline) {
        pipelines.put(pipeline.getExecutionId(), pipeline);
    }

    private void initState(PipelineContext pipeline) {
        ExecutionState state = new ExecutionState();
        state.setState(StateEnum.STAGING);
        pipeline.setState(state);
    }

    private void executePipeline(boolean parallel, PipelineContext pipeline) {
        TaskFactory.createAndExecuteTask(() -> {
            try {
                execute(pipeline, parallel);
            } catch (EngineException e) {
                ExecutionState executionState = new ExecutionState(StateEnum.FAILED, e);
                pipeline.setState(executionState);
                logger.error("Pipeline " + pipeline.getExecutionId(), e);
            }
        });
    }

    private void execute(PipelineContext pipeline, boolean parallel) throws EngineException {
        validate(pipeline);
        Collection<ExecutionNode> executionGraph = topologicalSort(pipeline, parallel);
        pipeline.setPipelineGraph(executionGraph);
        stage(pipeline);
    }

    private String generateExecutionId(String pipelineName) {
        String pipelineNameID = pipelineName == null || pipelineName.isEmpty() ? "" : pipelineName;
        String currTime = Calendar.getInstance().getTimeInMillis() + "";
        return pipelineNameID + "_" + currTime;
    }


    private Collection<ExecutionNode> topologicalSort(PipelineContext pipeline, boolean parallel) {
        return parallel ? TopologicSorter.parallelSort(pipeline) : TopologicSorter.sequentialSort(pipeline);
    }

    private void validate(PipelineContext pipeline) throws EngineException {
        IPipelineDescriptor pipelineDescriptor = pipeline.getDescriptor();
        ValidateUtils.validateRepositories(pipelineDescriptor.getRepositories());
        ValidateUtils.validateOutputs(pipeline);
        ValidateUtils.validateSteps(pipeline);
        ValidateUtils.validateNonCyclePipeline(pipeline);
    }

    private String getPipelineWorkingDirectory(String executionId) {
        String workDirectory = this.workingDirectory + File.separatorChar + executionId;
        return workDirectory.replace(" ", "");
    }

}
