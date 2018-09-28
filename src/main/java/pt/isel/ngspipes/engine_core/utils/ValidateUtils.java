package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.entities.contexts.ComposeStepContext;
import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleStepContext;
import pt.isel.ngspipes.engine_core.entities.contexts.StepContext;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.exception.InputValidationException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.ISpreadDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.ICombineStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IInputStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IStrategyDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IOutputDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IParameterDescriptor;

import java.io.File;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public class ValidateUtils {

    public static void validateSteps(PipelineContext pipeline) throws EngineException {

        for (Map.Entry<String, StepContext> entry : pipeline.getStepsContexts().entrySet()) {
            StepContext stepCtx = entry.getValue();
            IStepDescriptor step = stepCtx.getStep();

            if (step.getSpread() != null)
                validateSpread(step.getId(), step.getInputs(), step.getSpread());

            if (stepCtx instanceof ComposeStepContext) {
                validatePipelineStep(((ComposeStepContext) stepCtx), pipeline);
            } else {
                validateCommandStep(((SimpleStepContext) stepCtx), pipeline);
            }

        }
    }

    public static void validateOutputs(PipelineContext pipeline) throws EngineException {
        validateNoDuplicatedOutputsIds(pipeline);
        validateOutputsExistence(pipeline);
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

    public static void validateNonCyclePipeline(PipelineContext pipeline) throws EngineException {
        Collection<Map.Entry<String, String>> previousPipelines = new LinkedList<>();
        validateNonCycle(pipeline, null, previousPipelines);
    }

    public static void validatePipelineState(PipelineContext pipelineContext) throws EngineException {
        ExecutionState executionState = pipelineContext.getState();
        if (executionState.getState().equals(StateEnum.FAILED))
            throw executionState.getException();
    }

    public static void validateResources(StepContext stepContext, PipelineContext pipeline) throws EngineException {
        int processors = Runtime.getRuntime().availableProcessors();
        long memory = Runtime.getRuntime().freeMemory();
        long disk = new File(stepContext.getEnvironment().getOutputsDirectory()).getFreeSpace();
        String executionId = pipeline.getExecutionId();

        if (processors < stepContext.getEnvironment().getCpu() ||
                processors < pipeline.getPipelineEnvironment().getCpu())
            throw new EngineException("Needed cpus aren't available to execute step: " + stepContext.getId()
                    + " from pipeline: " + executionId);

        if (memory < stepContext.getEnvironment().getMemory() * 1024 ||
                memory < pipeline.getPipelineEnvironment().getMemory() * 1024)
            throw new EngineException("Needed memory isn't available to execute step: " + stepContext.getId()
                    + " from pipeline: " + executionId);

        if (disk < stepContext.getEnvironment().getDisk() * 1024 * 1024 ||
                disk < pipeline.getPipelineEnvironment().getDisk() * 1024 * 1024)
            throw new EngineException("Needed disk space isn't available to execute step: " + stepContext.getId()
                    + " from pipeline: " + executionId);
    }

    public static void validateInputValues(Map<String, Collection<String>> inputsValues, String first, String second) throws EngineException {
        int sizeFirst = inputsValues.get(first).size();
        int sizeSecond = inputsValues.get(second).size();
        if (sizeFirst != sizeSecond)
            throw new EngineException("Inputs to spread values must have the same quantity.");
    }


    private static void validateSpread(String stepId, Collection<IInputDescriptor> inputs, ISpreadDescriptor spread) throws EngineException {
        validateSpreadInputsExistence(stepId, spread.getInputsToSpread(), inputs);
        validateSpreadStrategyInputsExistence(stepId, spread);
    }

    private static void validateSpreadStrategyInputsExistence(String stepId, ISpreadDescriptor spread) throws EngineException {
        if (spread.getInputsToSpread().size() < 2)
            throw new EngineException("Inputs to spread must be at least 2.");
        int count = validateStrategyInput(stepId, spread.getInputsToSpread(), spread.getStrategy());
        if (spread.getInputsToSpread().size() < count)
            throw new EngineException("Inputs to spread must be " + count + ".");
    }

    private static int validateStrategyInput(String stepId, Collection<String> inputsToSpread,
                                             IStrategyDescriptor strategy) throws EngineException {
        if (strategy == null)
            throw new EngineException("Spread malformed on step" + stepId + ".");

        if (strategy instanceof ICombineStrategyDescriptor) {
            ICombineStrategyDescriptor combined = (ICombineStrategyDescriptor) strategy;
            int count = validateStrategyInput(stepId, inputsToSpread, combined.getFirstStrategy());
            return count + validateStrategyInput(stepId, inputsToSpread, combined.getSecondStrategy());
        } else if (strategy instanceof IInputStrategyDescriptor) {
            validateInput(stepId, inputsToSpread, (IInputStrategyDescriptor) strategy);
            return 1;
        }

        return 0;
    }

    private static void validateInput(String stepId, Collection<String> inputsToSpread,
                                      IInputStrategyDescriptor strategy) throws EngineException {
        String inputName = strategy.getInputName();
        if (!inputsToSpread.contains(inputName))
            throw new EngineException("Error validating spread on step:" + stepId + ".Strategy input " + inputName
                    + " must be specified on spread inputs");
    }

    private static void validateSpreadInputsExistence(String stepId, Collection<String> inputsToSpread,
                                                      Collection<IInputDescriptor> inputs) throws EngineException {
        for (String inputName : inputsToSpread) {
            IInputDescriptor input = DescriptorsUtils.getInputByName(inputs, inputName);
            if (input == null)
                throw new EngineException("Using " + inputName + " as input to spread and " +
                        "isn't defined as input of step " + stepId + ".");
        }
    }

    private static void validatePipelineStep(ComposeStepContext stepCtx, PipelineContext pipeline) throws EngineException {
        IPipelineDescriptor stepPipelineDescriptor = stepCtx.getPipelineDescriptor();
        PipelineContext subPipeline = ContextFactory.create(pipeline.getExecutionId(), stepPipelineDescriptor,
                pipeline.getParameters(), new Arguments(),
                stepCtx.getEnvironment().getWorkDirectory());
        validateSteps(subPipeline);
    }

    private static void validateCommandStep(SimpleStepContext stepCtx, PipelineContext pipeline) throws EngineException {
        IStepDescriptor step = stepCtx.getStep();
        ICommandDescriptor commandDescriptor = stepCtx.getCommandDescriptor();

        for (IParameterDescriptor parameterDescriptor : commandDescriptor.getParameters()) {
            IInputDescriptor input = DescriptorsUtils.getInputByName(step.getInputs(), parameterDescriptor.getName());

            validateInput(stepCtx, pipeline, commandDescriptor, parameterDescriptor, input);
        }
    }

    private static void validateInput(SimpleStepContext stepCtx, PipelineContext pipeline,
                                      ICommandDescriptor commandDescriptor, IParameterDescriptor parameterDescriptor,
                                      IInputDescriptor input) throws EngineException {
        validateMandatory(stepCtx, pipeline, commandDescriptor, parameterDescriptor, input);
        if (input != null)
            validateInputType(pipeline, stepCtx, parameterDescriptor, input);
    }

    private static void validateMandatory(SimpleStepContext stepCtx, PipelineContext pipeline,
                                          ICommandDescriptor commandDescriptor, IParameterDescriptor parameterDescriptor,
                                          IInputDescriptor input) throws EngineException {
        String depends = parameterDescriptor.getDepends();
        if (depends != null && !depends.isEmpty()) {
            validateDependent(stepCtx.getStep(), commandDescriptor.getParameters(), parameterDescriptor, input, pipeline.getParameters());
        } else {
            validateRequired(stepCtx.getStep(), parameterDescriptor, input);
        }
    }

    private static void validateInputType(PipelineContext pipeline, SimpleStepContext stepCtx,
                                          IParameterDescriptor parameterDescriptor, IInputDescriptor input) throws EngineException {
        String paramType = parameterDescriptor.getType();

        if (paramType.equals("composed")) {
            validateComposedInput(stepCtx.getStep().getInputs(), parameterDescriptor);
            return;
        }

        if (stepCtx.getStep().getSpread() != null && stepCtx.getStep().getSpread().getInputsToSpread().contains(input.getInputName()))
            paramType += "[]";

        if (input instanceof IChainInputDescriptor) {
            validateChainInputType(pipeline, paramType, input);
        } else {
            Object inputValue = DescriptorsUtils.getInputValue(input, pipeline.getParameters());
            validateInput(parameterDescriptor.getName(), paramType, inputValue);
        }
    }

    private static void validateComposedInput(Collection<IInputDescriptor> inputs, IParameterDescriptor parameterDescriptor) throws EngineException {
        for (IParameterDescriptor subParameter : parameterDescriptor.getSubParameters()) {
            IInputDescriptor inputByName = DescriptorsUtils.getInputByName(inputs, subParameter.getName());
            if (subParameter.isRequired() && inputByName == null)
                throw new EngineException("Input " + subParameter.getName() + " wasn't specified and is require.");
        }
    }

    private static void validateChainInputType(PipelineContext pipeline, String paramType, IInputDescriptor input) throws EngineException {
        IChainInputDescriptor chainInput = (IChainInputDescriptor) input;
        StepContext stepCtx = pipeline.getStepsContexts().get(chainInput.getStepId());

        String oputputType;

        if (stepCtx instanceof SimpleStepContext) {
            oputputType = getOutputTypeFromTool(chainInput, (SimpleStepContext) stepCtx);
        } else {
            oputputType = getOutputTypeFromPipeline(pipeline, chainInput, stepCtx.getStep().getId());
        }
        if (!paramType.equals(oputputType) && (paramType.equalsIgnoreCase("file_prefix") && !oputputType.contains("file")))
            throw new EngineException("Chained input: " + input.getInputName() + " type doesn't verify with output: "
                    + oputputType + " type.");
    }

    private static String getOutputTypeFromPipeline(PipelineContext pipeline, IChainInputDescriptor chainInput,
                                                    String stepId) throws EngineException {

        PipelineContext currPipeline = ContextFactory.createSubPipeline(stepId, pipeline);
        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : pipeline.getDescriptor().getOutputs()) {
            if (output.getOutputName().equals(chainInput.getOutputName())) {
                StepContext insideStepCtx = currPipeline.getStepsContexts().get(output.getStepId());
                if (insideStepCtx instanceof SimpleStepContext) {
                    return getOutputTypeFromTool(chainInput, (SimpleStepContext) insideStepCtx);
                } else {
                    PipelineContext nextPipeline = ContextFactory.createSubPipeline(insideStepCtx.getStep().getId(), pipeline);
                    return getOutputTypeFromPipeline(nextPipeline, chainInput, insideStepCtx.getStep().getId());
                }
            }
        }

        throw new EngineException("Error validating chain input " + chainInput.getInputName());
    }

    private static String getOutputTypeFromTool(IChainInputDescriptor chainInput,
                                                SimpleStepContext stepCtx) throws EngineException {
        ICommandDescriptor command = stepCtx.getCommandDescriptor();
        IOutputDescriptor output = DescriptorsUtils.getOutputFromCommand(command, chainInput.getOutputName());
        return output.getType();
    }

    private static void validateNonCycle(PipelineContext pipeline, IPipelineRepositoryDescriptor pipelineRepoDesc,
                                         Collection<Map.Entry<String, String>> previousPipelines) throws EngineException {

        if (pipelineRepoDesc != null) {
            String pipelineName = pipeline.getDescriptor().getName();
            String location = pipelineRepoDesc.getLocation();

            AbstractMap.SimpleEntry<String, String> currentPipeline = new AbstractMap.SimpleEntry<>(pipelineName, location);
            if (previousPipelines.contains(currentPipeline))
                throw new EngineException("It is not permit cycles on pipeline." + getCycleMessage(previousPipelines, currentPipeline));
            else
                previousPipelines.add(currentPipeline);
        }

        for (Map.Entry<String, StepContext> entry : pipeline.getStepsContexts().entrySet()) {
            StepContext stepCtx = entry.getValue();

            if (stepCtx instanceof ComposeStepContext) {

                IPipelineDescriptor subPipelineDesc = ((ComposeStepContext) stepCtx).getPipelineDescriptor();
                String workDirectory = stepCtx.getEnvironment().getWorkDirectory();
                Map<String, Object> parameters = pipeline.getParameters();
                Arguments arguments = new Arguments();
                PipelineContext subPipeline = ContextFactory.create(pipeline.getExecutionId(), subPipelineDesc,
                        parameters, arguments, workDirectory);
                validateNonCycle(subPipeline, pipelineRepoDesc, new LinkedList<>(previousPipelines));
            }
        }
    }

    private static String getCycleMessage(Collection<Map.Entry<String, String>> previousPipelines,
                                          AbstractMap.SimpleEntry<String, String> currentPipeline) {
        StringBuilder cycle = new StringBuilder();
        StringBuilder tab = new StringBuilder();

        for (Map.Entry entry : previousPipelines) {
            cycle.append(tab).append(entry.getValue()).append(" - ").append(entry.getKey()).append("\n");
            tab.append("\t");
        }

        cycle.append(tab).append(currentPipeline.getValue()).append(" - ").append(currentPipeline.getKey());
        return cycle.toString();
    }

    private static void validateOutputsExistence(PipelineContext pipeline) throws EngineException {

        IPipelineDescriptor pipelineDescriptor = pipeline.getDescriptor();

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor : pipelineDescriptor.getOutputs()) {

            StepContext stepCtx = pipeline.getStepsContexts().get(outputDescriptor.getStepId());
            String outputName = outputDescriptor.getOutputName();

            if (stepCtx instanceof SimpleStepContext) {
                validateExistenceOutputOnCommandStep((SimpleStepContext) stepCtx, outputName);
            } else if (stepCtx instanceof ComposeStepContext) {
                validateExistenceOutputOnPipelineStep((ComposeStepContext) stepCtx, outputName);
            } else {
                throw new EngineException("No existent implementation was found for verifying output " + outputDescriptor.getName());
            }
        }
    }

    private static void validateExistenceOutputOnPipelineStep(ComposeStepContext stepCtx, String outputName) {
        IPipelineDescriptor stepPipelineDescriptor = stepCtx.getPipelineDescriptor();
        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : stepPipelineDescriptor.getOutputs())
            if (output.getOutputName().equals(outputName))
                break;
    }

    private static void validateExistenceOutputOnCommandStep(SimpleStepContext stepCtx, String outputName) throws EngineException {
        IOutputDescriptor output = DescriptorsUtils.getOutputFromCommand(stepCtx.getCommandDescriptor(), outputName);

        if (output.getValue().contains("$"))
            validateDependentOutput(output, stepCtx);
    }

    private static void validateDependentOutput(IOutputDescriptor output, SimpleStepContext stepCtx) throws EngineException {

        String outputValue = output.getValue();

        if (outputValue.indexOf("$") != outputValue.lastIndexOf("$")) {
            String[] splittedByDependency = outputValue.split("$");
            for (String val : splittedByDependency) {
                validateExistentOfDependentInput(stepCtx, val);
            }
        } else {
            String val = outputValue.substring(outputValue.indexOf("$") + 1);
            validateExistentOfDependentInput(stepCtx, val);
        }


    }

    private static void validateExistentOfDependentInput(SimpleStepContext stepCtx, String str) throws EngineException {
        boolean contains = isOutputDependentInputSpecified(str, stepCtx.getStep().getInputs());
        if (!contains)
            throw new EngineException("Error validating output, dependent input wasn't specified.");

    }

    private static void validateNoDuplicatedOutputsIds(PipelineContext pipeline) throws EngineException {
        Collection<String> outputsIds = new LinkedList<>();

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor : pipeline.getDescriptor().getOutputs()) {
            if (outputsIds.contains(outputDescriptor.getOutputName()))
                throw new EngineException("Outputs ids can be duplicated. Id: " + outputDescriptor.getOutputName() + " is duplicated.");
            outputsIds.add(outputDescriptor.getName());
        }
    }

    private static void validateDependent(IStepDescriptor step, Collection<IParameterDescriptor> parameterDescriptors,
                                          IParameterDescriptor parameterDescriptor, IInputDescriptor input,
                                          Map<String, Object> parameters) throws EngineException {
        String depends = parameterDescriptor.getDepends().replace("$", "");
        IInputDescriptor inputByName = DescriptorsUtils.getInputByName(step.getInputs(), depends);
        IParameterDescriptor rootParameterDescriptor = DescriptorsUtils.getParameterById(parameterDescriptors, depends);
        if (input != null && (inputByName == null || rootParameterDescriptor == null))
            throw new EngineException("Input: " + parameterDescriptor.getName() + " depends on " + depends +
                    ".No input with name: " + depends + " was found.");
        if (parameterDescriptor.getDependentValues() != null && !parameterDescriptor.getDependentValues().isEmpty()) {
            Object rootValue = DescriptorsUtils.getInputValue(inputByName, parameters);
            boolean hasDependentValue = parameterDescriptor.getDependentValues().contains(rootValue.toString());
            if (input != null && !hasDependentValue)
                throw new EngineException("Input: " + parameterDescriptor.getName() + " depends on " + depends +
                        "but value: " + rootValue + " is not one of its dependent values.");
            else if (hasDependentValue && parameterDescriptor.isRequired() && input == null)
                throw new EngineException("Input: " + parameterDescriptor.getName() + " on step: " + step.getId() + " is required.");

        }
    }

    private static void validateRequired(IStepDescriptor step, IParameterDescriptor parameterDescriptor,
                                         IInputDescriptor input) throws EngineException {
        if (parameterDescriptor.isRequired() && input == null)
            throw new EngineException("Input: " + parameterDescriptor.getName() +
                    " is required and is not defined on step: " + step.getId() + ".");
    }

    private static void validateInput(String paramName, String paramType, Object inputValue) throws EngineException {
        try {
            TypeValidator.VALIDATORS.get(paramType).validate(inputValue);
        } catch (NullPointerException | InputValidationException e) {
            throw new EngineException("Input: " + paramName + " type isn't correct", e);
        }
    }

    public static boolean isOutputDependentInputSpecified(String value, Collection<IInputDescriptor> inputs) {
        boolean contains = false;
        for (IInputDescriptor input : inputs) {
            if (value.contains(input.getInputName())) {
                contains = true;
            }
        }
        return contains;
    }

    public static boolean isOutputDependentInputTypeNoPrimitive(String value, SimpleStepContext step) {
        boolean nonPrimitive = false;
        for (IInputDescriptor input : step.getStep().getInputs()) {
            IParameterDescriptor param = DescriptorsUtils.getParameterById(step.getCommandDescriptor().getParameters(), input.getInputName());
            if (value.contains(input.getInputName()) && (param.getType().equalsIgnoreCase("file") || param.getType().equalsIgnoreCase("directory"))) {
                nonPrimitive = true;
            }
        }
        return nonPrimitive;
    }
}
