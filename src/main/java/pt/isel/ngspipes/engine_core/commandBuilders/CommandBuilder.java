package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleStepContext;
import pt.isel.ngspipes.engine_core.entities.contexts.StepContext;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.utils.DescriptorsUtils;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.SimpleInputDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IParameterDescriptor;

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Supplier;

abstract class CommandBuilder implements ICommandBuilder {

    private Collection<String> visitedInputs = new LinkedList<>();
    private Collection<IInputDescriptor> buildInputs = new LinkedList<>();

    @FunctionalInterface
    interface TriFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    String buildCommand(PipelineContext pipelineContext, String stepId,
                        TriFunction<SimpleStepContext, Object, Object> func) throws CommandBuilderException {
        SimpleStepContext stepCtx = (SimpleStepContext) pipelineContext.getStepsContexts().get(stepId);
        ICommandDescriptor cmdDescriptor = stepCtx.getCommandDescriptor();
        StringBuilder sb = new StringBuilder(cmdDescriptor.getCommand());
        Collection<IInputDescriptor> inputs = new LinkedList<>(stepCtx.getStep().getInputs());

        visitedInputs = new LinkedList<>();
        buildInputs = new LinkedList<>();
        for (IInputDescriptor input : inputs) {
            if (visitedInputs.contains(input.getInputName()))
                continue;
            sb.append(" ");
            IParameterDescriptor paramDesc = DescriptorsUtils.getParameterById(cmdDescriptor.getParameters(), input.getInputName());
            if (paramDesc != null) {
                setInputValue(pipelineContext, stepId, sb, input, paramDesc, func);
            } else {
                paramDesc = getParentParameter(cmdDescriptor.getParameters(), input);
                if (paramDesc != null)
                    setInputValue(pipelineContext, stepId, sb, input, paramDesc, func);
            }
        }

        stepCtx.getStep().setInputs(buildInputs);
        return sb.toString();
    }

    Object getChainFileValue(SimpleStepContext stepCtx, Object value) {
        String valStr = value.toString();
        String separator = File.separatorChar + "";
        int begin = valStr.lastIndexOf(separator) != -1 ? valStr.lastIndexOf(separator) : valStr.lastIndexOf(separator);
        valStr = valStr.substring(begin + 1);
        return stepCtx.getEnvironment().getWorkDirectory() + separator + valStr;
    }

    private IParameterDescriptor getParentParameter(Collection<IParameterDescriptor> parameters, IInputDescriptor input) {
        for (IParameterDescriptor param : parameters) {
            if (param.getSubParameters() != null && !param.getSubParameters().isEmpty()) {
                Collection<IParameterDescriptor> subParameters = param.getSubParameters();
                for (IParameterDescriptor subParam : subParameters) {
                    if (subParam.getName().equalsIgnoreCase(input.getInputName()))
                        return param;
                }
            }
        }
        return null;
    }

    private void setInputValue(PipelineContext pipelineContext, String stepId, StringBuilder sb,
                               IInputDescriptor input,
                               IParameterDescriptor paramDesc, TriFunction<SimpleStepContext, Object, Object> func) throws CommandBuilderException {
        SimpleStepContext stepCtx = (SimpleStepContext) pipelineContext.getStepsContexts().get(stepId);
        if(paramDesc.getType().equals("composed")) {
            buildComposeInput(pipelineContext, stepId, sb, paramDesc, func);
        } else {
            setInputValue(pipelineContext, stepId, stepCtx, sb, input, paramDesc, func);
        }
    }

    private void setInputValue(PipelineContext pipelineContext, String stepId, SimpleStepContext stepCtx,
                               StringBuilder sb, IInputDescriptor input, IParameterDescriptor paramDesc,
                               TriFunction<SimpleStepContext, Object, Object> func) throws CommandBuilderException {
        Object value = getValue(pipelineContext, stepId, input);
        if (paramDesc.getType().equalsIgnoreCase("file") ) {
            value = getChainFileValue(pipelineContext, input, value);
            value = getFileInputValue(stepCtx, func, value);
        } else if (paramDesc.getType().equalsIgnoreCase("flag")) {
            value = "";
        }
        updateInput(input, value);
        buildInput(sb, paramDesc, value);
    }

    private void updateInput(IInputDescriptor input, Object value) {
        String inputName = input.getInputName();
        SimpleInputDescriptor input1 = new SimpleInputDescriptor();
        input1.setInputName(inputName);
        input1.setValue(value);
        buildInputs.add(input1);
    }

    private Object getChainFileValue(PipelineContext pipelineContext, IInputDescriptor input, Object value) {
        if (input instanceof ChainInputDescriptor) {
            ChainInputDescriptor input1 = (ChainInputDescriptor) input;
            StepContext chainStepCtx = pipelineContext.getStepsContexts().get(input1.getStepId());
            value = chainStepCtx.getOutputs().get(input1.getOutputName());
        }
        return value;
    }

    private Object getFileInputValue(SimpleStepContext stepCtx, TriFunction<SimpleStepContext, Object, Object> func,
                                     Object value) throws CommandBuilderException {
        try {
            value = func.apply(stepCtx, value);
        } catch (Exception e) {
            throw new CommandBuilderException("", e);
        }
        return value;
    }

    void buildComposeInput(PipelineContext pipelineContext, String stepId, StringBuilder sb,
                           IParameterDescriptor paramDesc, TriFunction<SimpleStepContext, Object, Object> func)
                            throws CommandBuilderException {

        sb.append(paramDesc.getPrefix());
        String separator = paramDesc.getSeparator();
        for (IParameterDescriptor subParamDesc : paramDesc.getSubParameters()) {
            StepContext stepContext = pipelineContext.getStepsContexts().get(stepId);
            IInputDescriptor subIn = DescriptorsUtils.getInputByName(stepContext.getStep().getInputs(), subParamDesc.getName());
            if (subIn != null) {
                setInputValue(pipelineContext, stepId, (SimpleStepContext) stepContext, sb, subIn, subParamDesc, func);
                visitedInputs.add(subIn.getInputName());
                sb.append(separator);
            } else if (subParamDesc.isRequired()) {
                throw new CommandBuilderException("Input " + subParamDesc.getName() + " is required on step " + stepId);
            }
        }
        sb.deleteCharAt(sb.lastIndexOf(separator));
    }

    void buildInput(StringBuilder sb, IParameterDescriptor paramDesc, Object inputValue) {
        sb  .append(getParameterProperty(paramDesc::getPrefix))
            .append(inputValue)
            .append(getParameterProperty(paramDesc::getSuffix));
    }

    private String getParameterProperty(Supplier<String> supplier) {
        String value = supplier.get();
        return value != null ? value : "";
    }

    Object getValue(PipelineContext pipelineCtx, String stepId, IInputDescriptor subIn) throws CommandBuilderException {
        Object inputValue;
        try {
            inputValue = DescriptorsUtils.getInputValue(subIn, pipelineCtx);
        } catch (EngineException e) {
            throw new CommandBuilderException("Error building command for step " + stepId + ". ", e);
        }
        return inputValue;
    }
}
