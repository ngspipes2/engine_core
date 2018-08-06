package pt.isel.ngspipes.engine_core.entities.contexts;

import pt.isel.ngspipes.engine_core.commandBuilders.ICommandBuilder;
import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IExecutionContextDescriptor;

public class SimpleStepContext extends StepContext {

    ICommandDescriptor commandDescriptor;
    ICommandBuilder commandBuilder;
    IExecutionContextDescriptor executionContextDescriptor;

    public SimpleStepContext(String id, Environment environment, IStepDescriptor step,
                             ICommandDescriptor commandDescriptor, ICommandBuilder commandBuilder, IExecutionContextDescriptor execCtxDesc) {
        super(id, environment, step);
        this.commandDescriptor = commandDescriptor;
        this.commandBuilder = commandBuilder;
        this.executionContextDescriptor = execCtxDesc;
    }

    public ICommandDescriptor getCommandDescriptor() { return commandDescriptor; }
    public ICommandBuilder getCommandBuilder() { return commandBuilder; }
    public IExecutionContextDescriptor getExecutionContextDescriptor() { return executionContextDescriptor; }
}
