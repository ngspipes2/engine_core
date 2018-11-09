package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.contexts.ExecutionContext;
import pt.isel.ngspipes.engine_core.entities.contexts.Job;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;

import java.io.File;
import java.util.Map;

public class DockerCommandBuilder extends CommandBuilder {

    // 1: engineStepOutputDir 2:engineStepInputDir 3:imageName 4:stepCommand
    private static final String DOCKER_CMD = "sudo docker run -w /sharedOutputs/ -v %1$s:/sharedOutputs/:rw -v " +
                                             "%2$s:/sharedInputs/:rw %3$s %4$s";
    private static final String DOCKER_IMG_NAME_KEY = "uri";
    private static final String DOCKER_IMG_TAG_KEY = "tag";

    @Override
    public String build(Pipeline pipeline, String stepId) throws CommandBuilderException {
        SimpleJob stepCtx = (SimpleJob) pipeline.getJobById(stepId);
        Environment environment = stepCtx.getEnvironment();
        String dockerImage = getDockerImageName(stepCtx.getExecutionContext());
        String executionCommand = buildCommand(pipeline, stepId, this::getDockerInputValue);
        return String.format(DOCKER_CMD, environment.getOutputsDirectory(),
                                            pipeline.getEnvironment().getWorkDirectory(),
                                        dockerImage, executionCommand);
    }

    private String getDockerInputValue(Job stepCtx, String value) {
        String separator = File.separatorChar + "";
        int begin = value.lastIndexOf(separator);
        String inputName = begin != -1 ? value.substring(begin + 1) : value;
        return separator + "sharedInputs" + separator + stepCtx.getId() + separator + inputName;
    }

    private String getDockerImageName(ExecutionContext execContext) throws CommandBuilderException {
        Map<String, Object> config = execContext.getConfig();
        if (!config.containsKey(DOCKER_IMG_NAME_KEY))
            throw new CommandBuilderException("Docker execution context must contain a configuration (uri) specifying docker image.");
        String uri = config.get(DOCKER_IMG_NAME_KEY).toString();
        String tag = config.get(DOCKER_IMG_TAG_KEY).toString();
        return uri + ((tag != null) ? ":" + tag : "");
    }

}
