package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.Job;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;

import java.util.Map;

public interface ICommandBuilder {

    String build(Pipeline pipeline, Job job, String fileSeparator, Map<String, Object> contextConfig) throws CommandBuilderException;
}
