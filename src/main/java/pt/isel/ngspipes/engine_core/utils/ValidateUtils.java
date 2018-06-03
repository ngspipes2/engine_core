package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.Pipeline;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.exception.InputValidationException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IPipelineExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.ISpreadDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.ICombineStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IInputStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IStrategyDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.pipeline_repository.PipelinesRepositoryException;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IOutputDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IParameterDescriptor;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public class ValidateUtils {
    
    public static void validateSteps(Pipeline pipeline) throws EngineException {
        for (Map.Entry<String, IStepDescriptor> entry : pipeline.getSteps().entrySet()) {
            IStepDescriptor step = entry.getValue();
            String repositoryId = step.getExec().getRepositoryId();

            if(step.getSpread() != null)
                validateSpread(step.getId(), step.getInputs(), step.getSpread());

            if(pipeline.getPipelinesRepositories().containsKey(repositoryId)) {
                IPipelinesRepository repository = pipeline.getPipelinesRepositories().get(repositoryId);
                validatePipelineStep(step, repository, pipeline);
            } else {
                IToolsRepository repository = pipeline.getToolsRepositories().get(repositoryId);
                validateCommandStep(step, repository, pipeline);
            }
        }
    }

    public static void validateOutputs(Collection<pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor> outputs, IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters) throws EngineException {
        validateNoDuplicatedOutputsIds(outputs);
        validateOutputsExistence(outputs, pipelineDescriptor, parameters);
    }

    public static void validateRepositories(Collection<IRepositoryDescriptor> repositories) throws EngineException {
        Collection<String> repoId = new LinkedList<>();
        for (IRepositoryDescriptor repositoryDescriptor : repositories) {
            if (repoId.contains(repositoryDescriptor.getId()))
                throw new EngineException("Repositories ids can be duplicated. Id: " + repositoryDescriptor.getId() + " is duplicated.");
            else
                repoId.add(repositoryDescriptor.getId());
        }
    }

    public static void validateNonCyclePipeline(Pipeline pipeline) throws EngineException {
        Collection<Map.Entry<String, String>> previousPipelines = new LinkedList<>();
        validateNonCycle(pipeline, null, previousPipelines);
    }



    private static void validateSpread(String stepId, Collection<IInputDescriptor> inputs, ISpreadDescriptor spread) throws EngineException {
        validateSpreadInputsExistence(stepId, spread.getInputsToSpread(), inputs);
        validateSpreadStrategyInputsExistence(stepId, spread);
    }

    private static void validateSpreadStrategyInputsExistence(String stepId, ISpreadDescriptor spread) throws EngineException {
        validateStrategyInput(stepId, spread.getInputsToSpread(), spread.getStrategy());
    }

    private static void validateStrategyInput(String stepId, Collection<String> inputsToSpread, IStrategyDescriptor strategy) throws EngineException {
        if (strategy == null)
            throw new EngineException("Spread malformed on step" + stepId + ".");

        if (strategy instanceof ICombineStrategyDescriptor) {
            ICombineStrategyDescriptor combineStrategy = (ICombineStrategyDescriptor) strategy;
            validateStrategyInput(stepId, inputsToSpread, combineStrategy.getFirstStrategy());
            validateStrategyInput(stepId, inputsToSpread, combineStrategy.getSecondStrategy());
        } else if (strategy instanceof IInputStrategyDescriptor) {
            IInputStrategyDescriptor inputStrategyDescriptor = (IInputStrategyDescriptor) strategy;
            String inputName = inputStrategyDescriptor.getInputName();
            if (!inputsToSpread.contains(inputName))
                throw new EngineException("Error validating spread on step:" + stepId + ".Strategy input " + inputName
                        + " must be specified on spread inputs");
        }
    }

    private static void validateSpreadInputsExistence(String stepId, Collection<String> inputsToSpread, Collection<IInputDescriptor> inputs) throws EngineException {
        for (String inputName : inputsToSpread) {
            IInputDescriptor input = ToolsUtils.getInputByName(inputs, inputName);
            if (input == null)
                throw new EngineException("Using " + inputName + " as input to spread and " +
                        "isn't defined as input of step " + stepId + ".");
        }
    }

    private static void validatePipelineStep(IStepDescriptor step, IPipelinesRepository repository, Pipeline pipeline) throws EngineException {
        IPipelineExecDescriptor pipelineExecDescriptor = (IPipelineExecDescriptor) step.getExec();
        try {
            IPipelineDescriptor stepPipelineDescriptor = repository.get(pipelineExecDescriptor.getPipelineName());
            Pipeline subPipeline = PipelineFactory.create(stepPipelineDescriptor, pipeline.getParameters());
            validateSteps(subPipeline);
        } catch (PipelinesRepositoryException e) {
            throw new EngineException("Error loading inside step pipeline: " + step.getId() + ".", e);
        }
    }

    private static void validateCommandStep(IStepDescriptor step, IToolsRepository repository, Pipeline pipeline) throws EngineException {
        ICommandExecDescriptor commandExecDescriptor = (ICommandExecDescriptor) step.getExec();
        ICommandDescriptor commandDescriptor = ToolsUtils.getCommand(repository, commandExecDescriptor);

        for (IParameterDescriptor parameterDescriptor : commandDescriptor.getParameters()) {
            IInputDescriptor input = ToolsUtils.getInputByName(step.getInputs(), parameterDescriptor.getName());

            validateMandatory(step, pipeline, commandDescriptor, parameterDescriptor, input);
            validateInputType(pipeline, step, parameterDescriptor, input);
        }
    }

    private static void validateMandatory(IStepDescriptor step, Pipeline pipeline, ICommandDescriptor commandDescriptor, IParameterDescriptor parameterDescriptor, IInputDescriptor input) throws EngineException {
        String depends = parameterDescriptor.getDepends();
        if (depends != null && !depends.isEmpty()) {
            validateDependent(step, commandDescriptor.getParameters(), parameterDescriptor, input, pipeline.getParameters());
        } else {
            validateRequired(step, parameterDescriptor, input);
        }
    }

    private static void validateInputType(Pipeline pipeline, IStepDescriptor step, IParameterDescriptor parameterDescriptor, IInputDescriptor input) throws EngineException {
        String paramType = parameterDescriptor.getType();

        if(step.getSpread() != null && step.getSpread().getInputsToSpread().contains(input.getInputName()))
            paramType += "[]";

        if (input instanceof IChainInputDescriptor) {
            validateChainInputType(pipeline, paramType, input);
        } else {
            Object inputValue = ToolsUtils.getInputValue(input, pipeline.getParameters());
            validateInput(parameterDescriptor.getName(), paramType, inputValue);
        }
    }

    private static void validateChainInputType(Pipeline pipeline, String paramType, IInputDescriptor input) throws EngineException {
        IChainInputDescriptor chainInput = (IChainInputDescriptor) input;
        IStepDescriptor stepById = pipeline.getSteps().get(chainInput.getStepId());
        String repositoryId = stepById.getExec().getRepositoryId();

        String oputputType;
        if(pipeline.getToolsRepositories().containsKey(repositoryId)) {
            oputputType = getOutputTypeFromTool(pipeline, chainInput, stepById);
        } else {
            oputputType = getOutputTypeFromPipeline(pipeline, chainInput, stepById);
        }

        if (!paramType.equals(oputputType))
            throw new EngineException("Chained input: " + input.getInputName() + " type doesn't verify with output: "
                    + oputputType + " type.");
    }

    private static String getOutputTypeFromPipeline(Pipeline pipeline, IChainInputDescriptor chainInput, IStepDescriptor step) throws EngineException {

        Pipeline currPipeline = PipelineFactory.createSubPipeline(step, pipeline);
        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : pipeline.getDescriptor().getOutputs()) {
            if (output.getOutputName().equals(chainInput.getOutputName())) {
                IStepDescriptor insideStep = currPipeline.getSteps().get(output.getStepId());
                if (insideStep.getExec() instanceof ICommandExecDescriptor) {
                    return getOutputTypeFromTool(pipeline, chainInput, insideStep);
                } else {
                    Pipeline nextPipeline = PipelineFactory.createSubPipeline(insideStep, pipeline);
                    return getOutputTypeFromPipeline(nextPipeline, chainInput, insideStep);
                }
            }
        }

        throw new EngineException("Error validating chain input " + chainInput.getInputName());
    }

    private static String getOutputTypeFromTool(Pipeline pipeline, IChainInputDescriptor chainInput,
                                                IStepDescriptor step) throws EngineException {
        IToolsRepository toolsRepository = pipeline.getToolsRepositories().get(step.getExec().getRepositoryId());
        ICommandDescriptor command = ToolsUtils.getCommand(toolsRepository, (ICommandExecDescriptor) step.getExec());
        IOutputDescriptor output = ToolsUtils.getOutputFromCommand(command, chainInput.getOutputName());
        return output.getType();
    }

    private static void validateNonCycle(Pipeline pipeline, IPipelineRepositoryDescriptor pipelineRepoDesc,
                                  Collection<Map.Entry<String, String>> previousPipelines) throws EngineException {

        if(pipelineRepoDesc != null) {
            String pipelineName = pipeline.getDescriptor().getName();
            String location = pipelineRepoDesc.getLocation();

            AbstractMap.SimpleEntry<String, String> currentPipeline = new AbstractMap.SimpleEntry<>(pipelineName, location);
            if (previousPipelines.contains(currentPipeline))
                throw new EngineException("It is not permit cycles on pipeline." + getCycleMessage(previousPipelines, currentPipeline));
            else
                previousPipelines.add(currentPipeline);
        }

        for (Map.Entry<String, IStepDescriptor> entry : pipeline.getSteps().entrySet()) {
            IStepDescriptor step = entry.getValue();
            if (step.getExec() instanceof IPipelineExecDescriptor) {
                IPipelineExecDescriptor pipelineExecDesc = (IPipelineExecDescriptor) step.getExec();

                IPipelineDescriptor subPipelineDesc = getPipelineDescriptor(pipeline, pipelineExecDesc);

                Pipeline subPipeline = PipelineFactory.create(subPipelineDesc, pipeline.getParameters());
                validateNonCycle(subPipeline, pipelineRepoDesc, new LinkedList<>(previousPipelines));
            }
        }
    }

    private static IPipelineDescriptor getPipelineDescriptor(Pipeline pipeline, IPipelineExecDescriptor pipelineExecDesc) throws EngineException {
        String pipelineName = pipelineExecDesc.getPipelineName();
        String repositoryId = pipelineExecDesc.getRepositoryId();
        IPipelinesRepository pipelinesRepository = pipeline.getPipelinesRepositories().get(repositoryId);
        try {
            return pipelinesRepository.get(pipelineName);
        } catch (PipelinesRepositoryException e) {
            throw new EngineException("Error loading pipeline " + pipelineName, e);
        }
    }

    private static String getCycleMessage(Collection<Map.Entry<String, String>> previousPipelines, AbstractMap.SimpleEntry<String, String> currentPipeline) {
        StringBuilder cycle = new StringBuilder();
        StringBuilder tab = new StringBuilder();

        for (Map.Entry entry : previousPipelines) {
            cycle.append(tab).append(entry.getValue()).append(" - ").append(entry.getKey()).append("\n");
            tab.append("\t");
        }

        cycle.append(tab).append(currentPipeline.getValue()).append(" - ").append(currentPipeline.getKey());
        return cycle.toString();
    }

    private static void validateOutputsExistence(Collection<pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor> outputs, IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters) throws EngineException {

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor : outputs) {
            IStepDescriptor step = ToolsUtils.getStepById(pipelineDescriptor, outputDescriptor.getStepId());
            IRepositoryDescriptor repository = RepositoryUtils.getRepositoryById(pipelineDescriptor.getRepositories(), step.getExec().getRepositoryId());
            if (repository instanceof IToolRepositoryDescriptor) {
                validateExistenceOutputOnCommandStep(parameters, outputDescriptor, step, (IToolRepositoryDescriptor) repository);
            } else if(repository instanceof IPipelineRepositoryDescriptor) {
                validateExistenceOutputOnPipelineStep(parameters, outputDescriptor, step, (IPipelineRepositoryDescriptor) repository);
            } else {
                throw new EngineException("No existent implementation was found for repository: " + repository.getId());
            }
        }
    }

    private static void validateExistenceOutputOnPipelineStep(Map<String, Object> parameters, pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor, IStepDescriptor step, IPipelineRepositoryDescriptor repository) throws EngineException {
        IPipelinesRepository pipelinesRepository = RepositoryUtils.getPipelinesRepository(repository, parameters);
        IPipelineExecDescriptor pipelineExecDescriptor = (IPipelineExecDescriptor) step.getExec();
        try {
            IPipelineDescriptor stepPipelineDescriptor = pipelinesRepository.get(pipelineExecDescriptor.getPipelineName());

            for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : stepPipelineDescriptor.getOutputs())
                if (output.getOutputName().equals(outputDescriptor.getOutputName()))
                    break;
        } catch (PipelinesRepositoryException e) {
            throw new EngineException("Error loading inside step pipeline: " + step.getId() + ".");
        }
    }

    private static void validateExistenceOutputOnCommandStep(Map<String, Object> parameters, pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor, IStepDescriptor step, IToolRepositoryDescriptor repository) throws EngineException {
        IToolsRepository toolsRepository = RepositoryUtils.getToolsRepository(repository, parameters);
        ICommandExecDescriptor commandExecDescriptor = (ICommandExecDescriptor) step.getExec();
        ICommandDescriptor commandDescriptor = ToolsUtils.getCommand(toolsRepository, commandExecDescriptor);
        ToolsUtils.getOutputFromCommand(commandDescriptor, outputDescriptor.getOutputName());
    }

    private static void validateNoDuplicatedOutputsIds(Collection<pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor> outputs) throws EngineException {
        Collection<String> outputsIds = new LinkedList<>();

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor : outputs) {
            if (outputsIds.contains(outputDescriptor.getOutputName()))
                throw new EngineException("Outputs ids can be duplicated. Id: " + outputDescriptor.getOutputName() + " is duplicated.");
            outputsIds.add(outputDescriptor.getName());
        }
    }

    private static void validateDependent(IStepDescriptor step, Collection<IParameterDescriptor> parameterDescriptors, IParameterDescriptor parameterDescriptor, IInputDescriptor input, Map<String, Object> parameters) throws EngineException {
        String depends = parameterDescriptor.getDepends();
        IInputDescriptor inputByName =  ToolsUtils.getInputByName(step.getInputs(), depends);
        IParameterDescriptor rootParameterDescriptor = ToolsUtils.getParameterById(parameterDescriptors, depends);
        if (input != null && (inputByName == null || rootParameterDescriptor == null))
            throw new EngineException("Input: " + parameterDescriptor.getName() + " depends on " + depends +
                    ".No input with name: " + depends + " was found.");
        if (parameterDescriptor.isRequired() && input == null)
            throw new EngineException("Input: " + parameterDescriptor.getName() + " on step: " + step.getId() + " is required.");

        if (!parameterDescriptor.getDependentValues().isEmpty()) {
            Object rootValue = ToolsUtils.getInputValue(inputByName, parameters);
            if (!parameterDescriptor.getDependentValues().contains(rootValue.toString()))
                throw new EngineException("Input: " + parameterDescriptor.getName() + " depends on " + depends +
                        "but value: " + rootValue + " is not one of its dependent values.");
        }
    }

    private static void validateRequired(IStepDescriptor step, IParameterDescriptor parameterDescriptor, IInputDescriptor input) throws EngineException {
        if (parameterDescriptor.isRequired() && input == null)
            throw new EngineException("Input: " + parameterDescriptor.getName() +
                    " is required and is not defined on step: " + step.getId() + ".");
    }

    private static void validateInput(String paramName, String paramType, Object inputValue) throws EngineException {
        try {
            TypeValidator.VALIDATORS.get(paramType).validate(inputValue);
        } catch (InputValidationException e) {
            throw new EngineException("Input: " + paramName + " type isn't correct", e);
        }
    }

}
