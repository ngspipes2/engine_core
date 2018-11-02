package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;

public class LocalCommandBuilder extends CommandBuilder {

    @Override
    public String build(Pipeline pipeline, String stepId)
                        throws CommandBuilderException {
        return buildCommand(pipeline, stepId, this::getChainFileValue);
    }

}
