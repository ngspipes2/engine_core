package pt.isel.ngspipes.engine_core.interfaces;

import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.entities.Arguments;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.util.Map;

public interface IEngine {

    Pipeline execute(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters, Arguments arguments) throws EngineException;
    Pipeline execute(String intermediateRepresentation, Arguments arguments) throws EngineException;
    boolean stop(String executionId) throws EngineException;
    boolean clean(String executionId) throws EngineException;
    boolean cleanAll() throws EngineException;
//    void getPipelineOutputs(String executionId, String outputDirectory) throws EngineException;
//    Status getStatus(String executionId);

}
