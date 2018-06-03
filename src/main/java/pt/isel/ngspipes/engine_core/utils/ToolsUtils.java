package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IParameterInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ISimpleInputDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IOutputDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IParameterDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IToolDescriptor;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;
import utils.ToolRepositoryException;

import java.util.Collection;
import java.util.Map;

public class ToolsUtils {


    public static ICommandDescriptor getCommand(IToolsRepository toolsRepository, ICommandExecDescriptor commandExecDescriptor) throws EngineException {

        String toolName = commandExecDescriptor.getToolName();
        String commandName = commandExecDescriptor.getCommandName();
        try {
            IToolDescriptor toolDescriptor = toolsRepository.get(toolName);
            ICommandDescriptor commandByName = getCommandByName(toolDescriptor.getCommands(), commandName);
            if (commandByName != null)
                return commandByName;
        } catch (ToolRepositoryException e) {
            throw new EngineException("Not tool found with name: " + toolName, e);
        }

        throw new EngineException("Not command: " + commandName + " was found for tool: " + toolName);
    }

    public static ICommandDescriptor getCommandByName(Collection<ICommandDescriptor> commands, String name) {

        for (ICommandDescriptor commandDescriptor :commands) {
            if (commandDescriptor.getName().equals(name))
                return commandDescriptor;
        }

        return null;
    }

    public static IStepDescriptor getStepById(IPipelineDescriptor pipelineDescriptor, String stepId) throws EngineException {

        for (IStepDescriptor step : pipelineDescriptor.getSteps())
            if (step.getId().equals(stepId))
                return step;
        throw new EngineException("No step was found for id: " + stepId);
    }

    public static IOutputDescriptor getOutputFromCommand(ICommandDescriptor commandDescriptor, String outputName) throws EngineException {

        for (IOutputDescriptor outputDescriptor : commandDescriptor.getOutputs())
            if (outputDescriptor.getName().equals(outputName))
                return outputDescriptor;
        throw new EngineException("Output " + outputName + " not found on " + commandDescriptor.getName());
    }

    public static IParameterDescriptor getParameterById(Collection<IParameterDescriptor> parameterDescriptors, String name) {

        for (IParameterDescriptor parameterDescriptor : parameterDescriptors)
            if (parameterDescriptor.getName().equals(name))
                return parameterDescriptor;
        return null;
    }

    public static IInputDescriptor getInputByName(Collection<IInputDescriptor> inputs, String name) {

        for (IInputDescriptor inputDescriptor : inputs) {
            if (inputDescriptor.getInputName().equals(name))
                return inputDescriptor;
        }

        return null;
    }

    public static Object getInputValue(IInputDescriptor input, Map<String, Object> parameters) throws EngineException {
        if (input instanceof IParameterInputDescriptor)
            return parameters.get(((IParameterInputDescriptor) input).getParameterName());
        if (input instanceof ISimpleInputDescriptor)
            return ((ISimpleInputDescriptor) input).getValue();

        throw new EngineException("No implementation exist to get the input: " + input.getInputName() + " value");
    }

}
