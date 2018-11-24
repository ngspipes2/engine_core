package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.value.IParameterValueDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.value.ISimpleValueDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.value.IValueDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.CommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IPipelineExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IParameterInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ISimpleInputDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.pipeline_repository.PipelinesRepositoryException;
import pt.isel.ngspipes.tool_descriptor.interfaces.*;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;
import utils.ToolsRepositoryException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DescriptorsUtils {

    public static IToolDescriptor getTool(IToolsRepository repo, String toolName, String stepId) throws EngineException {
        try {
            return repo.get(toolName);
        } catch (ToolsRepositoryException e) {
            throw new EngineException("Erro getting tool for step " + stepId, e);
        }
    }

    public static ICommandDescriptor getCommand(IToolsRepository toolsRepository, ICommandExecDescriptor commandExecDescriptor) throws EngineException {

        String toolName = commandExecDescriptor.getToolName();
        String commandName = commandExecDescriptor.getCommandName();
        try {
            IToolDescriptor toolDescriptor = toolsRepository.get(toolName);
            ICommandDescriptor commandByName = getCommandByName(toolDescriptor.getCommands(), commandName);
            if (commandByName != null)
                return commandByName;
        } catch (ToolsRepositoryException e) {
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

    public static IOutputDescriptor getOutputFromCommand(ICommandDescriptor commandDescriptor, String outputName) throws EngineException {

        for (IOutputDescriptor outputDescriptor : commandDescriptor.getOutputs())
            if (outputDescriptor.getName().equals(outputName))
                return outputDescriptor;
        return null;
    }

    public static IPipelineDescriptor getPipelineDescriptor(IPipelinesRepository repo, String pipelineName) throws EngineException {
        try {
            return repo.get(pipelineName);
        } catch (PipelinesRepositoryException e) {
            throw new EngineException("Error getting pipeline " + pipelineName + " descriptor", e);
        }
    }

    public static pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor getOutputFromPipeline(IPipelineDescriptor pipelineDescriptor, String outputName) {
        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : pipelineDescriptor.getOutputs())
            if (output.getName().equals(outputName))
                return output;
        return null;
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

    public static IExecutionContextDescriptor getExecutionContext(Collection<IExecutionContextDescriptor> execCtxs,
                                                                  IValueDescriptor exec, Map<String, Object> params) {

        String execName = getExecutionContextName(exec, params);
        for (IExecutionContextDescriptor execCtx : execCtxs)
            if (execCtx.getName().equalsIgnoreCase(execName))
                return execCtx;
        return null;
    }

    public static IStepDescriptor getStepById(IPipelineDescriptor pipelineDescriptor, String stepId) {
        for (IStepDescriptor step : pipelineDescriptor.getSteps())
            if (step.getId().equals(stepId))
                return step;

        return null;
    }

    public static ICommandDescriptor getCommandDescriptor(IStepDescriptor step, IPipelineDescriptor pipelineDesc, Map<String, Object> parameters) throws EngineException {
        CommandExecDescriptor exec = (CommandExecDescriptor) step.getExec();
        IToolRepositoryDescriptor toolRepoDesc = DescriptorsUtils.getToolRepositoryDescriptorById(exec.getRepositoryId(), pipelineDesc.getRepositories());
        assert toolRepoDesc != null;
        IToolsRepository repo = RepositoryUtils.getToolsRepository(toolRepoDesc, parameters);
        return DescriptorsUtils.getCommand(repo, exec);
    }

    public static IPipelineDescriptor getPipelineDescriptor(IPipelineDescriptor pipelineDesc, Map<String, Object> params,
                                                             IStepDescriptor step) throws EngineException {
        IPipelineExecDescriptor exec = (IPipelineExecDescriptor) step.getExec();
        Collection<IRepositoryDescriptor> repositories = pipelineDesc.getRepositories();
        IPipelineRepositoryDescriptor repoDesc = DescriptorsUtils.getPipelineRepositoryDescribtorById(exec.getRepositoryId(), repositories);
        assert repoDesc != null;
        IPipelinesRepository pipeRepo = RepositoryUtils.getPipelinesRepository(repoDesc, params);
        return DescriptorsUtils.getPipelineDescriptor(pipeRepo, exec.getPipelineName());
    }

    public static IPipelineDescriptor getPipelineDescriptor(Map<String, IPipelinesRepository> pipelinesRepos,
                                                            IStepDescriptor step) throws EngineException {
        IPipelineExecDescriptor exec = (IPipelineExecDescriptor) step.getExec();
        IPipelinesRepository repo = pipelinesRepos.get(exec.getRepositoryId());
        return DescriptorsUtils.getPipelineDescriptor(repo, exec.getPipelineName());
    }

    public static IToolRepositoryDescriptor getToolRepositoryDescriptorById(String repoId, Collection<IRepositoryDescriptor> repos) {

        for (IRepositoryDescriptor repo : repos)
            if (repo.getId().equals(repoId))
                return (IToolRepositoryDescriptor) repo;
        return null;
    }

    static IPipelineRepositoryDescriptor getPipelineRepositoryDescribtorById(String repoId, Collection<IRepositoryDescriptor> repos) {

        for (IRepositoryDescriptor repo : repos)
            if (repo.getId().equals(repoId))
                return (IPipelineRepositoryDescriptor) repo;
        return null;
    }

    private static String getExecutionContextName(IValueDescriptor exec, Map<String, Object> params) {
        if (exec instanceof ISimpleValueDescriptor)
            return ((ISimpleValueDescriptor) exec).getValue().toString();
        if (exec instanceof IParameterValueDescriptor)
            return params.get(((IParameterValueDescriptor) exec).getParameterName()).toString();
        return null;
    }
}
