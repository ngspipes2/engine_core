package pt.isel.ngspipes.engine_core.interfaces;

import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.util.Map;

public interface IEngine {

    Pipeline execute(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters, Arguments arguments) throws EngineException;
    Pipeline execute(String intermediateRepresentation, Arguments arguments) throws EngineException;
    boolean stop(String executionId) throws EngineException;

}
