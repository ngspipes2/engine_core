package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.contexts.Input;
import pt.isel.ngspipes.engine_core.entities.contexts.Job;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;

import java.io.File;
import java.util.AbstractMap;
import java.util.List;

abstract class CommandBuilder implements ICommandBuilder {

    @FunctionalInterface
    interface TriFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    String fileSeparator;

    String buildCommand(Pipeline pipeline, Job job,
                        TriFunction<AbstractMap.SimpleEntry<Job,String>, String, String> func) throws CommandBuilderException {
        SimpleJob sJob = (SimpleJob) job;
        StringBuilder sb = new StringBuilder(sJob.getCommand());

        for (Input input : sJob.getInputs()) {
            sb.append(" ");
            setInputValue(pipeline, sJob, sb, input, func);
        }

        return sb.toString();
    }

    String getChainFileValue(AbstractMap.SimpleEntry<Job, String> entry, String value) {
        if (value.contains(","))
            return getFileArrayInputValue(entry, value);
        return getSimpleInputValue(entry, value);
    }

    String getFileArrayInputValue(AbstractMap.SimpleEntry<Job, String> entry, String value) {
        StringBuilder sb = new StringBuilder();
        String[] values = value.split(",");

        for (String val : values) {
            sb  .append(getSimpleInputValue(entry, val))
                    .append(",");
        }

        return sb.toString();
    }

    private String getSimpleInputValue(AbstractMap.SimpleEntry<Job, String> entry, String value) {
        int begin = value.lastIndexOf(fileSeparator) != -1 ?
                    value.lastIndexOf(fileSeparator) : value.lastIndexOf(fileSeparator);
        value = value.substring(begin + 1);
        return entry.getKey().getEnvironment().getWorkDirectory() + fileSeparator + value;
    }

    private void setInputValue(Pipeline pipeline, SimpleJob job, StringBuilder sb,
                               Input input, TriFunction<AbstractMap.SimpleEntry<Job, String>, String, String> func)
                               throws CommandBuilderException {
        String value = getInputValue(job, input, pipeline, func);

        sb  .append(input.getPrefix())
                .append(value);

        if (input.getType().equalsIgnoreCase("composed")) {
            List<Input> subInputs = input.getSubInputs();
            for (int idx = 0; idx < subInputs.size(); idx++) {
                Input in = subInputs.get(idx);
                String val = getInputValue(job, in, pipeline, func);
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

    private String getInputValue(SimpleJob job, Input input, Pipeline pipeline,
                                 TriFunction<AbstractMap.SimpleEntry<Job, String>, String, String> func) throws CommandBuilderException {
        String value = input.getValue();
        String type = input.getType();

        if (type.equalsIgnoreCase("file") || type.equalsIgnoreCase("file[]") ||
            (type.equalsIgnoreCase("directory") && input.getOriginStep().equals(job.getId()))) {
            value = getFileInputValue(job, func, value, pipeline);
        } else {
             if (type.equalsIgnoreCase("flag")) {
                value = "";
            }
        }

        return value;
    }

    private String getFileInputValue(Job job, TriFunction<AbstractMap.SimpleEntry<Job, String>, String, String> func,
                                     String value, Pipeline pipeline) throws CommandBuilderException {
        try {
            value = func.apply(new AbstractMap.SimpleEntry<>(job, pipeline.getEnvironment().getWorkDirectory()), value);
        } catch (Exception e) {
            throw new CommandBuilderException("", e);
        }
        return value;
    }

}
