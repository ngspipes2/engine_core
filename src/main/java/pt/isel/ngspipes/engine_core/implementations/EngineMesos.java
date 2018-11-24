package pt.isel.ngspipes.engine_core.implementations;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.StateEnum;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.executionReporter.ConsoleReporter;
import pt.isel.ngspipes.engine_core.tasks.Task;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class EngineMesos extends Engine {


    private static final String OUT_DIR = "/home/pipes";
//    private static final String OUT_DIR = "/home/centos/pipes";
//    private static final String  SSH_HOST = "127.0.0.1";
    private static final String  SSH_HOST = "10.0.2.9";
//    private static final String  SSH_HOST = "192.92.149.146";
//    private static final int  SSH_PORT = 5555;
    private static final int  SSH_PORT = 22;
    private static final String  SSH_USER = "root";
//    private static final String  SSH_USER = "centos";
    private static final String  SSH_PASSWORD = "p1p3s2357NGS";
    private static final String KEY_PATH = "/home/dantas/Desktop/main/privateJava";
//    private static final String KEY_PATH = "E:\\Work\\NGSPipes\\key\\privateJava";
    private static Collection<String> IGNORE_FILES = new LinkedList<>(Arrays.asList("." , ".."));
    private static final String TAG = "MesosEngine";

    private static final String CHRONOS_DEPENDENCY = "dependency";
    private static final String CHRONOS_ISO = "iso8601";
    private static final String CHRONOS_JOB = "job";

//    private final String chronosEndpoint = "http://192.92.149.146:4400/scheduler/";
    private final String chronosEndpoint = "http://10.0.2.9:4400/scheduler/";
//    private final String chronosEndpoint = "http://localhost:5055/scheduler/";
//    private final String chronosEndpoint = "http://localhost:5059/scheduler/";
    private final String BASE_DIRECTORY = "/home/pipes";
//    private final String BASE_DIRECTORY = "/home/centos/pipes";
    private static final String RUN_CMD = "%1$s";
    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
            File.separatorChar + "Engine";


    private final Map<String, Collection<Job>> TASKS_BY_EXEC_ID = new HashMap<>();

    private final ConsoleReporter reporter = new ConsoleReporter();


    EngineMesos(String workingDirectory) {
        super(workingDirectory, TAG);
    }

    public EngineMesos() { super(WORK_DIRECTORY, TAG); }


    @Override
    public void run(Pipeline pipeline, Collection<ExecutionNode> executionGraph) {
        TASKS_BY_EXEC_ID.put(pipeline.getName(), pipeline.getJobs());
        schedule(executionGraph, pipeline);
    }

    @Override
    List<String> getOutputValuesFromJob(String chainOutput, Job originJob) {
        throw new NotImplementedException();
    }

    @Override
    List<String> getOutputValuesFromSpreadJob(String chainOutput, Job originJob, String spreadId) {
        throw new NotImplementedException();
    }

    @Override
    public boolean stop(String executionId) throws EngineException {
        AtomicBoolean stopped = new AtomicBoolean(true);

        for (Job job : TASKS_BY_EXEC_ID.get(executionId)) {
            try {
                String url = chronosEndpoint + CHRONOS_JOB + "/" + job.getId();
                Task<Void> delete = TaskFactory.createAndExecuteTask(() -> {
                    try {
                        HttpUtils.delete(url);
                    } catch (IOException e) {
                        stopped.set(false);
                    }
                });
                boolean wait = delete.cancelledEvent.await(200);
                stopped.set(stopped.get() && wait);
            } catch (InterruptedException e) {
                ExecutionState state = new ExecutionState();
                state.setState(StateEnum.STOPPED);
            }
        }
        return stopped.get();
    }

    @Override
    public void copyPipelineInputs(Pipeline pipeline) throws EngineException {
        logger.trace(TAG + ":: Copying pipeline " + pipeline.getName() + " "
                + pipeline.getName() + " inputs.");

         updateEnvironment(pipeline.getEnvironment());
        ChannelSftp sftp = null;
        try {
            SSHConfig config = getSshConfig();
            sftp = SSHUtils.getChannelSftp(config);
            SSHUtils.createIfNotExist(pipeline.getEnvironment().getWorkDirectory(), sftp);
        } catch (JSchException | SftpException  e) {
            throw new EngineException("Error connecting server " + SSH_HOST);
        } finally {
            if(sftp != null) {
                sftp.disconnect();
            }
        }

        for (Job step : pipeline.getJobs()) {
            updateEnvironment(step.getEnvironment());
            uploadInputs(step, step.getInputs());
        }
    }

    public static Collection<String> getPipelineOutputs(String pipelineName) throws JSchException {
        String pipelinePath = OUT_DIR + pipelineName;
        Collection<String> outputsNames = new LinkedList<>();
        SSHConfig config = getSshConfig();
        ChannelSftp channelSftp = SSHUtils.getChannelSftp(config);

        try {
            channelSftp.cd(pipelinePath);
            Vector fileList = channelSftp.ls(pipelinePath);

            for(int i=0; i< fileList.size(); i++){
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) fileList.get(i);
                if (!IGNORE_FILES.contains(entry.getFilename()))
                    outputsNames.add(entry.getFilename());
            }
        } catch (SftpException e) {
            e.printStackTrace();
        } finally {
            if(channelSftp != null) {
                channelSftp.disconnect();
            }
        }
        return outputsNames;
    }

    private static SSHConfig getSshConfig() {
//        return new SSHConfig(SSH_USER, SSH_PASSWORD, KEY_PATH, SSH_HOST, SSH_PORT);
        return new SSHConfig(SSH_USER, SSH_PASSWORD, SSH_HOST, SSH_PORT);
    }



    private void schedule(Collection<ExecutionNode> executionGraph, Pipeline pipeline) {
        try {
            for (ExecutionNode node : executionGraph) {
                executeBranch(node, pipeline);
            }
        } catch (EngineException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(pipeline.getName(), state);
        }
    }

    private void executeBranch(ExecutionNode node, Pipeline pipeline) throws EngineException {
        runTask(node.getJob(), pipeline);
        scheduleChilds(node.getChilds(), pipeline);

    }

    private void scheduleChilds(List<ExecutionNode> childs, Pipeline pipeline) throws EngineException {
        for (ExecutionNode child : childs) {
            runTask(child.getJob(), pipeline);
            scheduleChilds(child.getChilds(), pipeline);
        }
    }


    private void runTask(Job job, Pipeline pipeline) throws EngineException {
        ValidateUtils.validatePipelineState(pipeline);
        ValidateUtils.validateResources(job, pipeline);

        if (job instanceof ComposeJob) {
            throw new NotImplementedException();
        }

        SimpleJob simpleJob = (SimpleJob) job;
        if (simpleJob.getSpread() != null) {
            LinkedList<ExecutionNode> graph = new LinkedList<>();
            expandSpreadJob(pipeline, simpleJob, graph);
            run(pipeline, graph);
        } else {
            execute(pipeline, simpleJob);
        }
    }

    private void execute(Pipeline pipeline, SimpleJob job) throws EngineException {
        copyChainInputs(job, pipeline);
        run(job, pipeline);
    }

    private void run(SimpleJob job, Pipeline pipeline) throws EngineException {
        String executeCmd = getExecutionCommand(job, pipeline);
        try {
            reporter.reportInfo("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName());
            IOUtils.createFolder(job.getEnvironment().getOutputsDirectory());
            String chronosJob = getChronosJob(executeCmd, job, pipeline.getName());
            String url = chronosEndpoint + CHRONOS_ISO;
            if (!job.getParents().isEmpty())
                url = chronosEndpoint + CHRONOS_DEPENDENCY;
            HttpUtils.post(url, chronosJob);
//            validateOutputs(job);
        } catch (IOException e) {
            logger.error("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            throw new EngineException("Error executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
        }
    }

    private String getChronosJob(String executeCmd, SimpleJob job, String executionId) {
        String jobDir = BASE_DIRECTORY + File.separatorChar + executionId + File.separatorChar + job.getId();
        String dockerUri = getDockerUri(job.getExecutionContext().getConfig());
        if (job.getParents().isEmpty())
            return getChronosJob(executeCmd, job, executionId, jobDir, dockerUri);
        else
            return getDependentChronosJob(executeCmd, job, executionId, jobDir, dockerUri);
    }

    private String getDependentChronosJob(String executeCmd, SimpleJob job, String executionId, String jobDir, String dockerUri) {
        return "{\"parents\":\"" + getParents(job.getParents()) + "\",\"name\": \"" + executionId + "_" + job.getId() + "\"," +
                " \"container\": {\"type\": \"DOCKER\",\"image\": \"" + dockerUri + "\",\n" +
                "\"network\": \"HOST\",\"volumes\": [ {\"containerPath\": \"" + BASE_DIRECTORY + File.separatorChar + "\"," +
                " \"hostPath\": \"" + BASE_DIRECTORY + File.separatorChar + "\"," +
                " \"mode\": \"RW\"}]}," +
                " \"cpus\": \"" + getCpus(job) + "\",\"mem\":\"" + getMemory(job) + "\",\"uris\": []," +
                "\"shell\": \"true\"," +
                "\"command\": \"mkdir -p " + jobDir + " && " +
                "cd " + jobDir + " && " +
                executeCmd + "\"}";
    }

    private int getMemory(SimpleJob job) {
        int memory = job.getEnvironment().getMemory();
        return memory == 0 ? 2048 : memory;
    }

    private float getCpus(SimpleJob job) {
        int cpu = job.getEnvironment().getCpu();
        return cpu == 0 ? 0.5f : cpu/10;
    }

    private String getParents(Collection<String> parents) {
        StringBuilder sb = new StringBuilder("[");

        for (String parent : parents) {
            sb.append(parent).append(",");
        }

        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");

        return sb.toString();
    }

    private String getChronosJob(String executeCmd, SimpleJob job, String executionId, String jobDir, String dockerUri) {
        return "{\"schedule\":\"R1//PT30M\",\"name\": \"" + executionId + "_" + job.getId() + "\"," +
                " \"container\": {\"type\": \"DOCKER\",\"image\": \"" + dockerUri + "\",\n" +
                "\"network\": \"HOST\",\"volumes\": [ {\"containerPath\": \"" + BASE_DIRECTORY + File.separatorChar + "\"," +
                " \"hostPath\": \"" + BASE_DIRECTORY + File.separatorChar + "\"," +
                " \"mode\": \"RW\"}]}," +
                " \"cpus\": \"" + getCpus(job) + "\",\"mem\": \"" + getMemory(job) + "\",\"uris\": []," +
                "\"shell\": \"true\"," +
                "\"command\": \"mkdir -p " + jobDir + " && " +
                "cd " + jobDir + " && " +
                executeCmd + "\"}";
    }

    private String getDockerUri(Map<String, Object> config) {
        String dockerUri = "";
        dockerUri = dockerUri + config.get("uri");
        if (config.containsKey("tag"))
            dockerUri = dockerUri + ":" + config.get("tag");
        return dockerUri;
    }

    private String  getExecutionCommand(SimpleJob job, Pipeline pipeline) throws EngineException {
        try {
            String command = CommandBuilderSupplier.getCommandBuilder("Local").build(pipeline, job);
//            String command = job.getCommandBuilder().build(pipeline, job.getId());
            return String.format(RUN_CMD, command);
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new EngineException("Error when building step", e);
        }
    }


    public static boolean isJobSuccess(String url, String jobName) {
        url = url + "jobs/search?name=" + jobName;
        try {
            String content = HttpUtils.get(url);
            System.out.println("Getting status for: " + jobName);
            ChronosJobStatusDto chronosJobStatusDto = getChronosJobStatusDto(content);
            return chronosJobStatusDto.successCount > 0;
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return false;
    }

    private static ChronosJobStatusDto getChronosJobStatusDto(String content) throws IOException {
        return getObjectMapper(new JsonFactory()).readValue(content, ChronosJobStatusDto[].class)[0];
    }

    private static ObjectMapper getObjectMapper(JsonFactory factory) {
        return new ObjectMapper(factory);
    }

    private void copyChainInputs(SimpleJob stepCtx, Pipeline pipeline) throws EngineException {

        String destDir = stepCtx.getEnvironment().getWorkDirectory() + File.separatorChar;

        for (Input inputCtx : stepCtx.getInputs()) {
            if (!inputCtx.getOriginStep().equals(stepCtx.getId())) {
                Job chainStep = pipeline.getJobById(inputCtx.getOriginStep());
                String outDir = chainStep.getEnvironment().getOutputsDirectory() + File.separatorChar;
                Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
                List<String> usedBy = outCtx.getUsedBy();

                SSHConfig config = getSshConfig();
                try {
                    if (usedBy != null) {
                        for (String dependent : usedBy) {
                            Output outputCtx = chainStep.getOutputById(dependent);
                            String value = outputCtx.getValue().toString();
                            SSHUtils.copy(outDir, destDir, value, config);
                        }
                    } else {
                        String value = outCtx.getValue().toString();
                        SSHUtils.copy(outDir, destDir, value, config);
                    }
                } catch (JSchException | InterruptedException | SftpException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void updatePipelineState(String executionId, ExecutionState state) {
        pipelines.get(executionId).setState(state);
    }

    private void updateEnvironment(Environment environment) {
        String localWorkDir = this.workingDirectory;
        environment.setWorkDirectory(environment.getWorkDirectory().replace(localWorkDir, BASE_DIRECTORY));
        environment.setOutputsDirectory(environment.getOutputsDirectory().replace(localWorkDir, BASE_DIRECTORY));
    }

    private void uploadInputs(Job step, List<Input> inputs) throws EngineException {
        for (Input input : inputs) {
            uploadInput(step, input);
            uploadInputs(step, input.getSubInputs());
        }
    }

    private void uploadInput(Job step, Input input) throws EngineException {
        if (input.getType().equalsIgnoreCase("file") || input.getType().equalsIgnoreCase("directory")) {
            if (input.getChainOutput() == null || input.getChainOutput().isEmpty()) {
                uploadInput(input.getValue(), step);
            }
        }
    }

    private void uploadInput(String input, Job stepCtx) throws EngineException {
        String inputName = input.substring(input.lastIndexOf(File.separatorChar) + 1);
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + File.separatorChar;
        try {
            SSHConfig config = getSshConfig();
            ChannelSftp channelSftp = SSHUtils.getChannelSftp(config);
            SSHUtils.upload(destInput, input, channelSftp);
        } catch (JSchException | UnsupportedEncodingException | FileNotFoundException | SftpException e) {
            throw new EngineException("Error copying input file " + inputName);
        }
    }

    private void validateOutputs(SimpleJob stepCtx) throws EngineException {
        try {
            Collection<String> outputs = getStepOutputs(stepCtx.getEnvironment().getOutputsDirectory());

            for (Output outCtx : stepCtx.getOutputs()) {
                String type = outCtx.getType();
                if (type.equalsIgnoreCase("file") || type.equalsIgnoreCase("directory")) {
                    String out = outCtx.getValue() + "";
                    String output = getOutput(outputs, out);
                    if (output.isEmpty())
                        throw new EngineException("Output " + outCtx.getName() +
                                " not found. Error running job " + stepCtx.getId());
                }
            }
        } catch (JSchException | SftpException e) {
            throw new EngineException("Error getting job " + stepCtx.getId() + " outputs", e);
        }
    }

    private String getOutput(Collection<String> outputs, String out) {
        String output = "";

        for (String outName : outputs) {
            if (outName.contains(out)) {
                output = outName;
            }
        }
        return output;
    }

    private Collection<String> getStepOutputs(String path) throws SftpException, JSchException {
        LinkedList<String> outputsNames = new LinkedList<>();
        ChannelSftp channelSftp = null;

        try {
            SSHConfig config = getSshConfig();
            channelSftp = SSHUtils.getChannelSftp(config);
            outputsNames.addAll(getStepOutputs(path, "", channelSftp));
        } finally {
            if(channelSftp != null) {
                channelSftp.disconnect();
            }
        }

        return outputsNames;
    }

    private Collection<? extends String> getStepOutputs(String path, String parentEntry, ChannelSftp channelSftp) throws SftpException {
        List<String> outputsNames = new LinkedList<>();

        channelSftp.cd(path);
        Vector fileList = channelSftp.ls(path);
        for(int i=0; i< fileList.size(); i++) {
            ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) fileList.get(i);

            String filename = entry.getFilename();
            if (!IGNORE_FILES.contains(filename)) {
                if (entry.getAttrs().isDir()) {
                    String pathDir = path + File.separatorChar + filename;
                    outputsNames.addAll(getStepOutputs(pathDir, filename, channelSftp));
                } else {
                    if (parentEntry.isEmpty())
                        outputsNames.add(filename);
                    else
                        outputsNames.add(parentEntry + File.separatorChar + filename);
                }
            }
        }

        return outputsNames;
    }

}
