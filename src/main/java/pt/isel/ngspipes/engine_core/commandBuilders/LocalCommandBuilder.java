package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;

public class LocalCommandBuilder extends CommandBuilder {

    @Override
    public String build(PipelineContext pipelineContext, String stepId)
                        throws CommandBuilderException {
        return buildCommand(pipelineContext, stepId, this::getChainFileValue);
    }

}
