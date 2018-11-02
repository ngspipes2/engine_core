package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.Input;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;

import java.io.File;
import java.util.List;

abstract class CommandBuilder implements ICommandBuilder {

    @FunctionalInterface
    interface TriFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    String buildCommand(Pipeline pipeline, String stepId,
                        TriFunction<SimpleJob, String, String> func) throws CommandBuilderException {
        SimpleJob stepCtx = (SimpleJob) pipeline.getJobById(stepId);
        StringBuilder sb = new StringBuilder(stepCtx.getCommand());

        for (Input input : stepCtx.getInputs()) {
            sb.append(" ");
            setInputValue(pipeline, stepId, sb, input, func);
        }

        return sb.toString();
    }

    String getChainFileValue(SimpleJob stepCtx, String value) {
        String separator = File.separatorChar + "";
        int begin = value.lastIndexOf(separator) != -1 ? value.lastIndexOf(separator) : value.lastIndexOf(separator);
        value = value.substring(begin + 1);
        return stepCtx.getEnvironment().getWorkDirectory() + separator + value;
    }

    private void setInputValue(Pipeline pipeline, String stepId, StringBuilder sb,
                               Input input, TriFunction<SimpleJob, String, String> func)
                               throws CommandBuilderException {
        SimpleJob stepCtx = (SimpleJob) pipeline.getJobById(stepId);
        String value = getInputValue(stepCtx, input, func);

        sb  .append(input.getPrefix())
                .append(value);

        if (input.getType().equalsIgnoreCase("composed")) {
            List<Input> subInputs = input.getSubInputs();
            for (int idx = 0; idx < subInputs.size(); idx++) {
                Input in = subInputs.get(idx);
                String val = getInputValue(stepCtx, in, func);
                if (val != null && !val.isEmpty()) {
                    sb.append(in.getPrefix())
                            .append(val)
                            .append(in.getSuffix());
                    if (idx < subInputs.size() - 1)
                        sb.append(input.getSeparator());
                }
            }
        }
        sb.append(input.getSuffix());
    }

    private String getInputValue(SimpleJob stepCtx, Input input, TriFunction<SimpleJob, String, String> func) throws CommandBuilderException {
        String value = input.getValue();
        String type = input.getType();
        if (type.equalsIgnoreCase("file") || (type.equalsIgnoreCase("directory") && !input.getOriginStep().equals(stepCtx.getId()))) {
            value = getFileInputValue(stepCtx, func, value);
        } else if (type.equalsIgnoreCase("flag")) {
            value = "";
        }

        return value;
    }

    private String getFileInputValue(SimpleJob stepCtx, TriFunction<SimpleJob, String, String> func,
                                     String value) throws CommandBuilderException {
        try {
            value = func.apply(stepCtx, value);
        } catch (Exception e) {
            throw new CommandBuilderException("", e);
        }
        return value;
    }

}
