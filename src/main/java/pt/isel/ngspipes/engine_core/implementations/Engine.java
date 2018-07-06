package pt.isel.ngspipes.engine_core.implementations;

import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.engine_core.utils.PipelineFactory;
import pt.isel.ngspipes.engine_core.utils.TopologicSorter;
import pt.isel.ngspipes.engine_core.utils.ValidateUtils;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
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




    protected abstract void run(Collection<ExecutionNode> executionGraph, Pipeline pipeline, String executionId);


    public void schedule(String executionId, IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                         Arguments arguments) throws EngineException {

                Pipeline pipeline = PipelineFactory.create(pipelineDescriptor, parameters);
                validate(pipeline);
                pipeline = PipelineFactory.create(pipeline, PipelineFactory.createJobs(pipeline, arguments));
                Collection<ExecutionNode> executionGraph = topologicalSort(pipeline, arguments.parallel);
                run(executionGraph, pipeline, executionId);
    }

    protected String generateExecutionId() throws EngineException {
        // gerar um hash até encontrar um que não entre em conflito com as diretorias existentes na working directory
        throw new NotImplementedException();
    }


    protected Collection<ExecutionNode> topologicalSort(Pipeline pipeline, boolean parallel) throws EngineException {
        return parallel ? TopologicSorter.parallelSort(pipeline) : TopologicSorter.sequentialSort(pipeline);
    }

    protected void validate(Pipeline pipeline) throws EngineException {
        IPipelineDescriptor pipelineDescriptor = pipeline.getDescriptor();
        ValidateUtils.validateRepositories(pipelineDescriptor.getRepositories());
        ValidateUtils.validateOutputs(pipelineDescriptor.getOutputs(), pipelineDescriptor, pipeline.getParameters());
        ValidateUtils.validateSteps(pipeline);
        ValidateUtils.validateNonCyclePipeline(pipeline);
    }

}
