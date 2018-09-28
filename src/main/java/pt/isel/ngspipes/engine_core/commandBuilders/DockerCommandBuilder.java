package pt.isel.ngspipes.engine_core.commandBuilders;

import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleStepContext;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_core.utils.IOUtils;
import pt.isel.ngspipes.tool_descriptor.interfaces.IExecutionContextDescriptor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DockerCommandBuilder extends CommandBuilder {

    // 1: engineStepOutputDir 2:engineStepInputDir 3:imageName 4:stepCommand
    private static final String DOCKER_CMD = "sudo docker run -w /sharedOutputs/ -v %1$s:/sharedOutputs/:rw -v " +
                                             "%2$s:/sharedInputs/:rw %3$s %4$s";
    private static final String DOCKER_IMG_NAME_KEY = "uri";
    private static final String DOCKER_IMG_TAG_KEY = "tag";

    @Override
    public String build(PipelineContext pipelineContext, String stepId) throws CommandBuilderException {
        SimpleStepContext stepCtx = (SimpleStepContext) pipelineContext.getStepsContexts().get(stepId);
        Environment environment = stepCtx.getEnvironment();
        IExecutionContextDescriptor execContext = stepCtx.getExecutionContextDescriptor();
        String dockerImage = getDockerImageName(execContext, stepId);
        String executionCommand = buildCommand(pipelineContext, stepId, this::getDockerInputValue);
        String command = String.format(DOCKER_CMD, environment.getOutputsDirectory(),
                                        pipelineContext.getPipelineEnvironment().getWorkDirectory(),
                                        dockerImage, executionCommand);
        return command;
    }

    private Object getDockerInputValue(SimpleStepContext stepCtx, Object value) {
        String valStr = value.toString();
        String separator = File.separatorChar + "";
        int begin = valStr.lastIndexOf(separator);
        String inputName = valStr.substring(begin + 1);
        return separator + "sharedInputs" + separator + stepCtx.getId() + separator + inputName;
    }

    private String getDockerImageName(IExecutionContextDescriptor execContext, String stepId) throws CommandBuilderException {
        Map<String, Object> config = execContext.getConfig();
        if (!config.containsKey(DOCKER_IMG_NAME_KEY))
            throw new CommandBuilderException("Docker execution context must contain a configuration (uri) specifying docker image.");
        String uri = config.get(DOCKER_IMG_NAME_KEY).toString();
        String tag = config.get(DOCKER_IMG_TAG_KEY).toString();
        return uri + ((tag != null) ? ":" + tag : "");
    }

}
