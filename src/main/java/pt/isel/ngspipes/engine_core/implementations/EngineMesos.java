package pt.isel.ngspipes.engine_core.implementations;

import com.github.brunomndantas.tpl4j.factory.TaskFactory;
import com.github.brunomndantas.tpl4j.task.Task;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.entities.factories.ChronosJobStatusFactory;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.executionReporter.ConsoleReporter;
import pt.isel.ngspipes.engine_core.utils.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class EngineMesos extends Engine {

    private static final String OUT_DIR = "/home/calmen";
//    private static final String OUT_DIR = "/home/pipes";
//    private static final String OUT_DIR = "/home/centos/pipes";
//    private static final String  SSH_HOST = "127.0.0.1";
//    private static final String  SSH_HOST = "10.0.2.9";
//    private static final String  SSH_HOST = "10.62.73.31";
    private static final String  SSH_HOST = "10.141.141.11";
//    private static final String  SSH_HOST = "192.92.149.146";
//    private static final int  SSH_PORT = 5555;
    private static final int  SSH_PORT = 22;
    private static final String  SSH_USER = "vagrant";
//    private static final String  SSH_USER = "calmen";
//    private static final String  SSH_USER = "root";
//    private static final String  SSH_USER = "centos";
//    private static final String  SSH_PASSWORD = "p1p3s2357NGS";
    private static final String  SSH_PASSWORD = "vagrant";
//    private static final String  SSH_PASSWORD = "ngs##19";
//    private static final String KEY_PATH = "/home/dantas/Desktop/main/privateJava";
//    private static final String KEY_PATH = "E:\\Work\\NGSPipes\\key\\privateJava";
    private static final String KEY_PATH = "E:\\\\Escola\\\\ISEL\\\\MEIC\\\\56_Semestre_Dissertacao\\\\vagrantMesos\\\\.vagrant\\\\machines\\\\\\ngspipes2\\\\virtualbox\\\\private_key";
    private static Collection<String> IGNORE_FILES = new LinkedList<>(Arrays.asList("." , ".."));
    private static final String TAG = "MesosEngine";

    private static final String CHRONOS_DEPENDENCY = "dependency";
    private static final String CHRONOS_ISO = "iso8601";
    private static final String CHRONOS_JOB = "jobs";
    private static final String CHRONOS_JOB_SEARCH = "jobs/search?name=";

//    private final String chronosEndpoint = "http://10.62.73.31:4400/scheduler/";
    private final String chronosEndpoint = "http://10.141.141.11:4400/scheduler/";
//    private final String chronosEndpoint = "http://192.92.149.146:4400/scheduler/";
//    private final String chronosEndpoint = "http://10.0.2.9:4400/scheduler/";
//    private final String chronosEndpoint = "http://localhost:4400/scheduler/";
//    private final String chronosEndpoint = "http://localhost:5059/scheduler/";
//    private final String chronosEndpoint = "http://localhost:5055/scheduler/";
//    private final String BASE_DIRECTORY = "/home/pipes";
//    private final String BASE_DIRECTORY = "/home/calmen";
    private final String BASE_DIRECTORY = "/home/vagrant";
//    private final String BASE_DIRECTORY = "/home/centos/pipes";
    private static final String RUN_CMD = "%1$s";
    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
            File.separatorChar + "Engine";
    private static final String epsilon = "P1Y12M12D";


    private ChannelSftp channelSftp;
    private final Map<String, Collection<Job>> TASKS_BY_EXEC_ID = new HashMap<>();

    private final ConsoleReporter reporter = new ConsoleReporter();


    EngineMesos(String workingDirectory) {
        super(workingDirectory, TAG, "/");
    }

    public EngineMesos() { super(WORK_DIRECTORY, TAG, "/"); }


    @Override
    protected void configure(Pipeline pipeline) throws EngineException { }

    @Override
    public void run(Pipeline pipeline, Collection<ExecutionNode> executionGraph) {
        try {
            List<Job> executionJobs = new LinkedList<>();
            storeJobs(pipeline, executionJobs);
            sortByExecutionOrder(executionGraph, pipeline, executionJobs);
            execute(pipeline, executionJobs);
            scheduleFinishPipeline(pipeline, executionJobs);
        } catch (EngineException | IOException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(pipeline.getName(), state);
        }
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
                Task<Void> delete = TaskFactory.createAndStart("stop" + executionId, () -> {
                    try {
                        HttpUtils.delete(url);
                    } catch (IOException e) {
                        stopped.set(false);
                        throw e;
                    }
                });
                boolean wait = delete.getStatus().cancelledEvent.await(200);
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
        logger.trace(TAG + ":: Copying pipeline " + pipeline.getName() + " inputs.");

        updateEnvironment(pipeline.getEnvironment());
        ChannelSftp sftp = null;
        try {
            SSHConfig config = getSshConfig();
            sftp = SSHUtils.getChannelSftp(config);
            SSHUtils.createIfNotExist(BASE_DIRECTORY, pipeline.getEnvironment().getWorkDirectory(), sftp, fileSeparator);
        } catch (JSchException | SftpException  e) {
            throw new EngineException("Error connecting server " + SSH_HOST);
        } finally {
            if(sftp != null) {
                sftp.disconnect();
                try {
                    sftp.getSession().disconnect();
                } catch (JSchException e) {
                    throw new EngineException("Error copying inputs.", e);
                }
            }
        }

        for (Job job : pipeline.getJobs()) {
            updateEnvironment(job.getEnvironment());
            uploadInputs(pipeline.getName(), job, job.getInputs());
        }
    }

    @Override
    public boolean clean(String executionId) throws EngineException {
        throw new NotImplementedException();
    }

    @Override
    public boolean cleanAll() throws EngineException {
        throw new NotImplementedException();
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



    private void scheduleFinishPipeline(Pipeline pipeline, List<Job> executionJobs) throws IOException {
        String jobId = "success" + pipeline.getName();
        String successJob = getSuccessChronosJob(executionJobs, jobId, pipeline.getName());
        HttpUtils.post(chronosEndpoint + CHRONOS_DEPENDENCY, successJob);
        waitUntilJobFinish(new String[] {jobId, pipeline.getName()});
        pipeline.getState().setState(StateEnum.SUCCESS);
        reporter.reportInfo("Pipeline Finished");
    }

    private String getSuccessChronosJob(List<Job> executionJobs, String jobId, String pipelineName) throws IOException {
        List<String> parents = executionJobs.stream().map((job)-> "validateOutputs_" + pipelineName + "_" + job.getId()).collect(Collectors.toList());
        return getDependentChronosJob("ls", parents, jobId);
    }

    private static SSHConfig getSshConfig() {
        return new SSHConfig(SSH_USER, SSH_PASSWORD, KEY_PATH, SSH_HOST, SSH_PORT);
//        return new SSHConfig(SSH_USER, SSH_PASSWORD, SSH_HOST, SSH_PORT);
    }

    private void storeJobs(Pipeline pipeline, List<Job> executionJobs) {
        if (!TASKS_BY_EXEC_ID.containsKey(pipeline.getName()))
            TASKS_BY_EXEC_ID.put(pipeline.getName(), executionJobs);
        else
            TASKS_BY_EXEC_ID.get(pipeline.getName()).addAll(executionJobs);
    }

    private void sortByExecutionOrder(Collection<ExecutionNode> executionGraph, Pipeline pipeline, List<Job> executionJobs) {
        executionGraph.forEach((node) -> executionJobs.add(node.getJob()));
        for (ExecutionNode node : executionGraph) {
            addByDependency(pipeline, executionJobs, node);
        }
    }

    private void addByDependency(Pipeline pipeline, List<Job> executionJobs, ExecutionNode node) {
        for (ExecutionNode child : node.getChilds()) {
            List<Job> parents = new LinkedList<>();
            Job job = child.getJob();
            if (executionJobs.contains(job))
                continue;
            job.getParents().forEach((parent) -> {
                Job parentJobById = pipeline.getJobById(parent);
                if (!parents.contains(parentJobById))
                    parents.add(parentJobById);
            });
            int present = (int) parents.stream().filter(executionJobs::contains).count();
            if (present == parents.size()) {
                executionJobs.add(job);
                addByDependency(pipeline, executionJobs, child);
            }
        }
    }

    private void execute(Pipeline pipeline, List<Job> executionJobs) throws EngineException {
        ValidateUtils.validatePipelineState(pipeline);

        for (Job job : executionJobs) {
            ValidateUtils.validateResources(job, pipeline);
            SimpleJob sJob = (SimpleJob) job;
            copyChainInputs(sJob, pipeline);
            run(sJob, getChronosJob(sJob, pipeline), pipeline);
        }
    }

    private String getChronosJob(SimpleJob job, Pipeline pipeline) throws EngineException {
        String executeCmd = getExecutionCommand(job, pipeline) + "; ls";
        return getChronosJob(executeCmd, job, pipeline.getName());
    }

    private void run(SimpleJob job, String chronosJob, Pipeline pipeline) throws EngineException {
        try {
            reporter.reportInfo("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName());
            IOUtils.createFolder(job.getEnvironment().getOutputsDirectory());
            String url = chronosEndpoint + CHRONOS_ISO;
            if (!job.getParents().isEmpty())
                url = chronosEndpoint + CHRONOS_DEPENDENCY;
            HttpUtils.post(url, chronosJob);
            String jobId = pipeline.getName() + "_" + job.getId();
            validateOutputs(job, pipeline.getName());
            runInconclusiveDependencies(job, pipeline, this::waitUntilJobFinish, jobId, pipeline.getName());
        } catch (IOException e) {
            logger.error("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            throw new EngineException("Error executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
        }
    }

    private void waitUntilJobFinish(String[] ids) {
        String url = chronosEndpoint + CHRONOS_JOB_SEARCH + ids[0];
        ChronosJobStatusDto chronosJobStatusDto = null;
        try {
            do {
                String content = HttpUtils.get(url);
                chronosJobStatusDto = ChronosJobStatusFactory.getChronosJobStatusDto(content);
                Thread.sleep(5000);
            } while (chronosJobStatusDto.successCount <= 0);
        } catch (IOException | InterruptedException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(ids[1], state);
        }
    }

    private String getChronosJob(String executeCmd, SimpleJob job, String executionId) throws EngineException {
        String jobDir = job.getEnvironment().getOutputsDirectory();
        try {
            if (job.getParents().isEmpty())
                return getChronosJob(executeCmd, job, executionId, jobDir);
            else
                return getDependentChronosJob(executeCmd, job, executionId, jobDir);
        } catch (IOException ex) {
            throw new EngineException("Error creating chronos job.", ex);
        }
    }

    private String getDependentChronosJob(String executeCmd, SimpleJob job, String executionId, String jobDir) throws EngineException, IOException {
        String command = "mkdir -p " + jobDir + " && " + "cd " + jobDir + " && " + executeCmd;
        String name = executionId + "_" + job.getId();
        List<String> parents = getParents(job, executionId);
        if (job.getExecutionContext().getContext().equalsIgnoreCase("DOCKER")) {
            Container container = getContainerInfo(job);
            DockerChronosJob dockerChronosJob = new DockerChronosJob(parents, epsilon, getCpus(job), command, name, getMemory(job), container);
            return JacksonUtils.serialize(dockerChronosJob);
        } else  {
            ChronosJob chronosJob = new ChronosJob(parents, epsilon, getCpus(job), command, name, getMemory(job));
            return JacksonUtils.serialize(chronosJob);
        }
    }

    private String getDependentChronosJob(String executeCmd, List<String> parents, String id) throws IOException {
        String epsilon = "P1Y12M12D";
        String shell = "true";
        String name = id + "_" + (parents.size() == 1 ? parents.get(0) : "");
        ChronosJob chronosJob = new ChronosJob(parents, epsilon, "0.5f", shell, executeCmd, name, "512");
        return JacksonUtils.serialize(chronosJob);
    }

    private String getChronosJob(String executeCmd, SimpleJob job, String executionId, String jobDir) throws EngineException, IOException {
        String schedule = "R1//P1Y";
        String command = "cd " + jobDir + " && ls && " + executeCmd;
        String name = executionId + "_" + job.getId();
        if (job.getExecutionContext().getContext().equalsIgnoreCase("DOCKER")) {
            Container container = getContainerInfo(job);
            DockerChronosJob dockerChronosJob = new DockerChronosJob(schedule, epsilon, getCpus(job), command, name, getMemory(job), container);
            return JacksonUtils.serialize(dockerChronosJob);
        } else  {
            ChronosJob chronosJob = new ChronosJob(schedule, epsilon, getCpus(job), command, name, getMemory(job));
            return JacksonUtils.serialize(chronosJob);
        }
    }

    private Container getContainerInfo(SimpleJob job) {
        List<Volume > volumes = new LinkedList<>();
        String paths = BASE_DIRECTORY + fileSeparator;
        Volume volume = new Volume(paths, paths);
        volumes.add(volume);
        String image = getDockerUri(job.getExecutionContext().getConfig());
        return new Container(image, volumes);
    }

    private String getDockerUri(Map<String, Object> config) {
        String dockerUri = "";
        dockerUri = dockerUri + config.get("uri");
        if (config.containsKey("tag"))
            dockerUri = dockerUri + ":" + config.get("tag");
        return dockerUri;
    }

    private String getMemory(SimpleJob job) {
        int memory = job.getEnvironment().getMemory();
        int mem = memory == 0 ? 2048 : memory;
        return mem + "";
    }

    private String getCpus(SimpleJob job) {
        int cpu = job.getEnvironment().getCpu();
        float value = cpu == 0 ? 0.5f : cpu / 10;
        return value + "";
    }

    private List<String> getParents(SimpleJob job, String pipelineName) throws EngineException {
        String uri = chronosEndpoint + CHRONOS_JOB + "/search?name=" + pipelineName + job.getId();
        try {
            String parentJobsName = HttpUtils.get(uri);
            return JacksonUtils.readPropertyValues(parentJobsName, "name");
        } catch (IOException e) {
            throw new EngineException("Error getting parent dependency", e);
        }
    }

    private String  getExecutionCommand(SimpleJob job, Pipeline pipeline) throws EngineException {
        try {
            String command = CommandBuilderSupplier.getCommandBuilder("Local").build(pipeline, job, fileSeparator, job.getExecutionContext().getConfig());
//            String command = job.getCommandBuilder().build(pipeline, job);
            return String.format(RUN_CMD, command);
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new EngineException("Error when building step", e);
        }
    }

    private void copyChainInputs(SimpleJob job, Pipeline pipeline) throws EngineException {
        String destDir = job.getEnvironment().getWorkDirectory() + fileSeparator;

        for (Input inputCtx : job.getInputs()) {
            if (!inputCtx.getOriginStep().equals(job.getId())) {
                Job chainStep = pipeline.getJobById(inputCtx.getOriginStep());
                String outDir = chainStep.getEnvironment().getOutputsDirectory() + fileSeparator;
                Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
                List<String> usedBy = outCtx.getUsedBy();
                String jobName = pipeline.getName() + job.getId() + "_cp_outputs" + chainStep.getId();

                if (usedBy != null) {
                    for (String dependent : usedBy) {
                        Output outputCtx = chainStep.getOutputById(dependent);
                        String value = outputCtx.getValue().toString();
                        copyInput(pipeline.getName() + "_" + chainStep.getId(), jobName + dependent, destDir, outDir, value);
                    }
                } else {
                    String value = outCtx.getValue().toString();
                    copyInput(pipeline.getName() + "_" + chainStep.getId(), jobName + outCtx.getName(), destDir, outDir, value);
                }
            }
        }
    }

    private void copyInput(String parent, String id, String destDir, String outDir, String value) throws EngineException {
        String createDestDir = "mkdir -p " + destDir;
        String copyInputs = " && cp -R " + outDir + value + " " + destDir;
        String changePermissions = " && chmod -R 777 " + destDir;
        parent = "validateOutputs_" + parent;
        try {
            String executeCmd = createDestDir + copyInputs + changePermissions;
            String copyTask = getDependentChronosJob(executeCmd, Collections.singletonList(parent), id);
            HttpUtils.post(chronosEndpoint + CHRONOS_DEPENDENCY, copyTask);
        } catch (IOException e) {
            logger.error("Error copying input: " + value , e);
            throw new EngineException("Error copying input: " + value , e);
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

    private void uploadInputs(String pipelineName, Job step, List<Input> inputs) throws EngineException {
        for (Input input : inputs) {
            uploadInput(pipelineName, step, input);
            uploadInputs(pipelineName, step, input.getSubInputs());
        }
    }

    private void uploadInput(String pipelineName, Job step, Input input) throws EngineException {
        if (input.getType().equalsIgnoreCase("file") || input.getType().equalsIgnoreCase("directory")) {
            if (input.getChainOutput() == null || input.getChainOutput().isEmpty()) {
                uploadInput(pipelineName, input, step);
            }
        }
    }

    private void uploadInput(String pipelineName, Input input, Job stepCtx) throws EngineException {
        String value = input.getValue();
        String inputName = value.substring(value.lastIndexOf(fileSeparator) + 1);
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + fileSeparator;
        try {
            SSHConfig config = getSshConfig();
            if (channelSftp == null)
                channelSftp = SSHUtils.getChannelSftp(config);
            String pipeline_dir = BASE_DIRECTORY + fileSeparator + pipelineName;
            SSHUtils.upload(pipeline_dir, destInput, value, channelSftp, fileSeparator);
            input.setValue(pipeline_dir + fileSeparator + value.substring(value.lastIndexOf(File.separatorChar) + 1));
        } catch (JSchException | UnsupportedEncodingException | FileNotFoundException | SftpException e) {
            throw new EngineException("Error copying input file " + inputName, e);
        }
    }

    private void validateOutputs(SimpleJob stepCtx, String executionId) throws EngineException {
        StringBuilder chronosJobCmd = new StringBuilder();
        String outputDir = stepCtx.getEnvironment().getOutputsDirectory();

        for (Output outCtx : stepCtx.getOutputs()) {
            String type = outCtx.getType();
            if (type.contains("ile") || type.contains("irectory")) {
                String out = outCtx.getValue() + "";
                if(!out.isEmpty()) {
                    if (chronosJobCmd.length() != 0)
                        chronosJobCmd.append(" && ");
                    chronosJobCmd.append("find ")
                                .append(outputDir + fileSeparator + out);
                }
            }
        }
        try {
            String parent = executionId + "_" + stepCtx.getId();
            String id = "validateOutputs";
            String chronosJob = getDependentChronosJob(chronosJobCmd.toString(), Collections.singletonList(parent), id);
            HttpUtils.post(chronosEndpoint + CHRONOS_DEPENDENCY, chronosJob);
        } catch (IOException e) {
            logger.error("Error step: " + stepCtx.getId() + " didn't produces all outputs." , e);
            throw new EngineException("Error step: " + stepCtx.getId() + " didn't produces all outputs." , e);
        }
    }

}
