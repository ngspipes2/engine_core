package pt.isel.ngspipes.engine_core.implementations;

import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.engine_core.utils.PipelineFactory;
import pt.isel.ngspipes.engine_core.utils.ValidateUtils;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class Engine implements IEngine {


    protected String workingDirectory;
    protected Map<String, ExecutionState> states;

    public Engine(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    @Override
    public String execute(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters, Arguments arguments) throws EngineException {
        String id = generateExecutionId();
        states.put(id, new ExecutionState());
        schedule(id, pipelineDescriptor, parameters, arguments);
        return id;
    }

    @Override
    public ExecutionState getState(String executionId) throws EngineException {
        if(states.containsKey(executionId))
            return states.get(executionId);

        throw new EngineException("No execution state found for: " + executionId);
    }



    protected String generateExecutionId() throws EngineException {
        // gerar um hash até encontrar um que não entre em conflito com as diretorias existentes na working directory
        throw new NotImplementedException();
    }

    protected void schedule(String executionId, IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                            Arguments arguments) throws EngineException {
        new Thread(() -> {
            try {
                Pipeline pipeline = PipelineFactory.create(pipelineDescriptor, parameters);
                validate(pipeline);
                pipeline = PipelineFactory.create(pipeline, PipelineFactory.createJobs(pipeline, arguments));
                List<ExecutionNode> executionGraph = topologicalSort(pipelineDescriptor, arguments.parallel);
                run(executionGraph, pipeline);
            } catch (EngineException e) {
                ExecutionState executionState = states.get(executionId);
                executionState.setException(e);
                executionState.setState(StateEnum.FAILED);
            }
        }).start();
    }

    protected void run(List<ExecutionNode> executionGraph, Pipeline pipeline) throws EngineException {



        throw new NotImplementedException();
    }



    protected List<ExecutionNode> topologicalSort(IPipelineDescriptor pipelineDescriptor, boolean parallel) throws EngineException {
        throw new NotImplementedException();
    }

    protected void validate(Pipeline pipeline) throws EngineException {
        IPipelineDescriptor pipelineDescriptor = pipeline.getDescriptor();
        ValidateUtils.validateRepositories(pipelineDescriptor.getRepositories());
        ValidateUtils.validateOutputs(pipelineDescriptor.getOutputs(), pipelineDescriptor, pipeline.getParameters());
        ValidateUtils.validateSteps(pipeline);
        ValidateUtils.validateNonCyclePipeline(pipeline);
    }


}
