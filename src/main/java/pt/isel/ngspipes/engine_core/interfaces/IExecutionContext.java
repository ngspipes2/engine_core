package pt.isel.ngspipes.engine_core.interfaces;

import java.util.Collection;

public interface IExecutionContext {

    String getName();
    String buildCommand(Collection<IInput> inputs, Collection<String> outputs);
    Collection<String> getInstallCommands();
}
