package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.Input;
import pt.isel.ngspipes.engine_core.entities.contexts.Job;
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
                        TriFunction<Job, String, String> func) throws CommandBuilderException {
        SimpleJob stepCtx = (SimpleJob) pipeline.getJobById(stepId);
        StringBuilder sb = new StringBuilder(stepCtx.getCommand());

        for (Input input : stepCtx.getInputs()) {
            sb.append(" ");
            setInputValue(pipeline, stepId, sb, input, func);
        }

        return sb.toString();
    }

    String getChainFileValue(Job stepCtx, String value) {
        String separator = File.separatorChar + "";
        int begin = value.lastIndexOf(separator) != -1 ? value.lastIndexOf(separator) : value.lastIndexOf(separator);
        value = value.substring(begin + 1);
        return stepCtx.getEnvironment().getWorkDirectory() + separator + value;
    }

    private void setInputValue(Pipeline pipeline, String stepId, StringBuilder sb,
                               Input input, TriFunction<Job, String, String> func)
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

    private String getInputValue(SimpleJob job, Input input, TriFunction<Job, String, String> func) throws CommandBuilderException {
        String value = input.getValue();
        String type = input.getType();

        if (!input.getOriginStep().equals(job.getId())) {
            value = getInputValueForChain(job, input, func, value, type);
        } else {
             if (type.equalsIgnoreCase("flag")) {
                value = "";
            }
        }

        return value;
    }

    private String getInputValueForChain(SimpleJob job, Input input, TriFunction<Job, String, String> func,
                                         String value, String type) throws CommandBuilderException {
        if (type.equalsIgnoreCase("directory")) {
            value = getFileInputValue(job, func, value);
        } else if (type.equalsIgnoreCase("file")) {
            value = getFileInputValue(job, func, value);
        } else if (input.getOriginJob().getSpread() != null) {
            value = getInputValueForSpread(job, input, func, value, type);
        }
        return value;
    }

    private String getInputValueForSpread(SimpleJob job, Input input, TriFunction<Job, String, String> func,
                                          String value, String type) throws CommandBuilderException {
        if (type.equalsIgnoreCase("file[]")) {
            String[] splittedVals = value.split(",");
            StringBuilder sb = new StringBuilder();
            for (String val : splittedVals) {
                sb.append(getFileInputValue(job, func, val)).append(",");
            }
            value = sb.toString();
        } else if (!type.equalsIgnoreCase("string")) {
            throw new CommandBuilderException("Input " + input.getName() + " build for incoming spread result and type "
                                                + type + "not supported.");
        }
        return value;
    }

    private String getFileInputValue(Job job, TriFunction<Job, String, String> func,
                                     String value) throws CommandBuilderException {
        try {
            value = func.apply(job, value);
        } catch (Exception e) {
            throw new CommandBuilderException("", e);
        }
        return value;
    }

}
