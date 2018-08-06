package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;

public interface ICommandBuilder {

    String build(PipelineContext pipelineContext, String stepId) throws CommandBuilderException;
}
