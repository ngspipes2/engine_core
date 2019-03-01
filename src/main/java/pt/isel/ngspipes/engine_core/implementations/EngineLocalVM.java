package pt.isel.ngspipes.engine_core.implementations;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.springframework.util.StringUtils;
import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.executionReporter.ConsoleReporter;
import pt.isel.ngspipes.engine_core.tasks.BasicTask;
import pt.isel.ngspipes.engine_core.tasks.Task;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class EngineLocalVM extends Engine {

    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
            File.separatorChar + "Engine";
    private static final String BASE_DIRECTORY = "/home/vagrant";
    private static final String SSH_HOST = "10.141.141.";
    private static final int SSH_PORT = 22;
    private static final String FILE_SEPARATOR = "/";
    private static final String SSH_USER = "vagrant";
    private final Map<String, Map<Job, BasicTask<Void>>> TASKS_BY_EXEC_ID = new HashMap<>();
    private static final String TAG = "LocalVMEngine";
    private static final String KEY_PATH = "%1$s\\.vagrant\\machines\\%2$s\\virtualbox\\private_key";
    private static final String configName = "config.json";

    private int ipAddressSuffix = 14;
    private final ConsoleReporter reporter = new ConsoleReporter();


    public EngineLocalVM() {
        super(WORK_DIRECTORY, TAG, FILE_SEPARATOR);
    }

    public EngineLocalVM(String workingDirectory) {
        super(workingDirectory, TAG, FILE_SEPARATOR);
    }

    @Override
    protected void configure(Pipeline pipeline) throws EngineException {
        logger.info("Configuring " + TAG);
        try {
            ipAddressSuffix++;
            createAndCopyVMFiles(pipeline);
            initVM(pipeline);
        } catch (IOException e) {
            throw new EngineException("Error initiating engine", e);
        }
    }

    @Override
    protected void run(Pipeline pipeline, Collection<ExecutionNode> graph) throws EngineException {
        if (!TASKS_BY_EXEC_ID.containsKey(pipeline.getName())) {
            TASKS_BY_EXEC_ID.put(pipeline.getName(), new HashMap<>());
            pipeline.getState().setState(StateEnum.RUNNING);
        }
        Map<Job, BasicTask<Void>> taskMap = getTasks(graph, pipeline, new HashMap<>());
        scheduleFinishPipelineTask(pipeline, taskMap);
        scheduleChildTasks(pipeline, taskMap);
        scheduleParentsTasks(graph, pipeline.getName(), taskMap);
    }

    @Override
    protected void copyPipelineInputs(Pipeline pipeline) throws EngineException {
        logger.trace(TAG + ":: Copying pipeline " + pipeline.getName() + " "
                + pipeline.getName() + " inputs.");

        for (Job job : pipeline.getJobs()) {
            copyInputs(job, job.getInputs());
        }
    }

    @Override
    List<String> getOutputValuesFromJob(String chainOutput, Job originJob) {
        Output out = originJob.getOutputById(chainOutput);
        String outputsDirectory = originJob.getEnvironment().getOutputsDirectory();
        String pattern = out.getValue().toString();
        return new LinkedList<>(IOUtils.getFileNamesByPattern(outputsDirectory, pattern));
    }

    @Override
    List<String> getOutputValuesFromSpreadJob(String chainOutput, Job originJob, String spreadId) {
        Output out = originJob.getOutputById(chainOutput);
        String outputsDirectory = originJob.getEnvironment().getOutputsDirectory();
        String patterns = out.getValue().toString();
        String appendAtEnd = "";
        if (out.getType().contains("ile[]")) {
            appendAtEnd = patterns.substring(patterns.lastIndexOf("]") + 1);
            patterns = patterns.replace(appendAtEnd, "");
        }
        int beginIdx = spreadId.lastIndexOf(originJob.getId()) + originJob.getId().length();
        int idx = Integer.parseInt(spreadId.substring(beginIdx));
        String pattern = SpreadJobExpander.getValues(patterns).get(idx) + appendAtEnd;
        List<String> fileNamesByPattern = IOUtils.getFileNamesByPattern(outputsDirectory + fileSeparator + spreadId, pattern);
        return new LinkedList<>(fileNamesByPattern);
    }

    @Override
    public boolean stop(String executionId) throws EngineException {
        boolean stopped = true;

        for (Map.Entry<Job, BasicTask<Void>> step : TASKS_BY_EXEC_ID.get(executionId).entrySet()) {
            step.getValue().cancel();
            try {
                stopped = stopped && step.getValue().cancelledEvent.await(200);
            } catch (InterruptedException e) {
                ExecutionState state = new ExecutionState();
                state.setState(StateEnum.STOPPED);
            }
        }
        return stopped;
    }

    @Override
    public boolean clean(String executionId) throws EngineException {
        throw new NotImplementedException();
    }

    @Override
    public boolean cleanAll() throws EngineException {
        throw new NotImplementedException();
    }



    private void copyInputs(Job job, List<Input> inputs) throws EngineException {
        for (Input input : inputs) {
            copyInput(job, input);
            copyInputs(job, input.getSubInputs());
        }
    }

    private void initVM(Pipeline pipeline) throws EngineException {
        String workDirectory = workingDirectory + File.separatorChar + pipeline.getName();
        ProcessRunner.runOnSpecificFolder("vagrant up" , reporter, workDirectory);
    }

    private void createAndCopyVMFiles(Pipeline pipeline) throws IOException {
        createVmConfigFile(pipeline);
        String vagrantFileName = "Vagrantfile";
        File source = getVagrantFile(vagrantFileName);
        String destPath = pipeline.getEnvironment().getWorkDirectory() + fileSeparator + vagrantFileName;
        IOUtils.copyFile(source.getAbsolutePath(), destPath);
    }

    private File getVagrantFile(String vagrantFileName) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(vagrantFileName).getFile());
    }

    private void createVmConfigFile(Pipeline pipeline) throws IOException {
        String workDirectory = pipeline.getEnvironment().getWorkDirectory();
        String vagrantConfig = getVmConfigFileContent(pipeline);
        IOUtils.writeFile(workDirectory + fileSeparator + configName, vagrantConfig);
    }

    private String getVmConfigFileContent(Pipeline pipeline) throws IOException {
        String ip_address = SSH_HOST + ipAddressSuffix;
        Environment env = pipeline.getEnvironment();
        int cpu = env.getCpu();
        cpu = cpu == 0 ? 1 : cpu;
        String name = pipeline.getName();
        String workDirectory = pipeline.getEnvironment().getWorkDirectory();
        VagrantConfig vagrantConfig = new VagrantConfig(ip_address, TAG + name, workDirectory, env.getMemory(), cpu, name);
        return JacksonUtils.serialize(vagrantConfig);
    }

    private void scheduleFinishPipelineTask(Pipeline pipeline, Map<Job, BasicTask<Void>> taskMap) {
        BasicTask<Void> task = (BasicTask<Void>) TaskFactory.createTask(() -> {
            try {
                pipeline.getState().setState(StateEnum.SUCCESS);
                reporter.reportInfo("Pipeline Finished");
                String workDirectory = pipeline.getEnvironment().getWorkDirectory();
                ProcessRunner.runOnSpecificFolder("vagrant destroy", reporter, workDirectory);
                TaskFactory.dispose();
            } catch (Exception e) {

            }
        });
        Task<Collection<Void>> tasks = TaskFactory.whenAllTasks(new ArrayList<>(taskMap.values()));
        tasks.then(task);
    }

    private Map<Job, BasicTask<Void>> getTasks(Collection<ExecutionNode> executionGraph, Pipeline pipeline, Map<Job, BasicTask<Void>> tasks) {
        for (ExecutionNode node : executionGraph) {
            Job job = node.getJob();
            BasicTask<Void> task = (BasicTask<Void>) TaskFactory.createTask(() -> {
                try {
                    runTask(job, pipeline);
                } catch (EngineException e) {
                    updateState(pipeline, job, e, StateEnum.FAILED);
                }
            });
            tasks.put(job, task);
            TASKS_BY_EXEC_ID.get(pipeline.getName()).put(job, task);
            getTasks(node.getChilds(), pipeline, tasks);
        }

        return tasks;
    }

    private void scheduleChildTasks(Pipeline pipeline, Map<Job, BasicTask<Void>> taskMap) {
        for (Map.Entry<Job, BasicTask<Void>> entry : taskMap.entrySet()) {
            if (!entry.getKey().getParents().isEmpty()) {
                runWhenAll(pipeline, entry.getKey(), taskMap);
            }
        }
    }

    private void updatePipelineState(String executionId, ExecutionState state) {
        pipelines.get(executionId).setState(state);
    }

    private void runWhenAll(Pipeline pipeline, Job job, Map<Job, BasicTask<Void>> taskMap) {
        Collection<Task<Void>> parentsTasks = getParentsTasks(pipeline, job.getParents());
        Task<Collection<Void>> tasks = TaskFactory.whenAllTasks(parentsTasks);
        tasks.then(taskMap.get(job));
    }

    private Collection<Task<Void>> getParentsTasks(Pipeline pipeline, Collection<String> parents) {
        Collection<Task<Void>> parentsTasks = new LinkedList<>();

        for (String parent : parents) {
            Map<Job, BasicTask<Void>> jobBasicTaskMap = TASKS_BY_EXEC_ID.get(pipeline.getName());
            jobBasicTaskMap.keySet().forEach((jobById) -> {
                if (jobById.getId().equalsIgnoreCase(parent)) {
                    BasicTask<Void> e = jobBasicTaskMap.get(jobById);
                    parentsTasks.add(e);
                }
            });
        }

        return parentsTasks;
    }

    private void scheduleParentsTasks(Collection<ExecutionNode> executionGraph, String executionId,
                                      Map<Job, BasicTask<Void>> taskMap) {
        try {
            executeParents(executionGraph, taskMap);
        } catch (EngineException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(executionId, state);
        }
    }

    private void executeParents(Collection<ExecutionNode> executionGraph, Map<Job, BasicTask<Void>> task)
            throws EngineException {
        for (ExecutionNode parentNode : executionGraph) {
            Job job = parentNode.getJob();
            if (!job.getParents().isEmpty())
                continue;
            try {
                logger.trace(TAG + ":: Executing step " + job.getId());
                task.get(job).run();
            } catch (Exception e) {
                logger.error(TAG + ":: Executing step " + job.getId(), e);
                throw new EngineException("Error executing step: " + job.getId(), e);
            }
        }
    }

    private void runTask(Job job, Pipeline pipeline) throws EngineException {
        ValidateUtils.validatePipelineState(pipeline);
        ValidateUtils.validateResources(job, pipeline);
        job.getState().setState(StateEnum.RUNNING);

        SimpleJob simpleJob = (SimpleJob) job;
        if (simpleJob.getSpread() != null) {
            LinkedList<ExecutionNode> graph = new LinkedList<>();
            SpreadJobExpander.expandSpreadJob(pipeline, simpleJob, graph, this::getOutputValues, fileSeparator);
            run(pipeline, graph);
        } else {
            execute(pipeline, simpleJob);
        }

        if (job.isInconclusive()) {
            job.setInconclusive(false);
            List<Job> childJobs = new LinkedList<>();
            for (Job childJob : job.getChainsTo()) {
                if (childJob.getSpread() != null) {
                    childJobs.addAll(SpreadJobExpander.getExpandedJobs(pipeline, (SimpleJob) childJob, new LinkedList<>(), this::getOutputValues, fileSeparator));
                } else {
                    childJobs.add(childJob);
                }
            }
            Collection<ExecutionNode> childGraph = TopologicSorter.parallelSort(pipeline, childJobs);
            run(pipeline, childGraph);
        }

        updateState(pipeline, job, null, StateEnum.SUCCESS);
    }

    private void execute(Pipeline pipeline, SimpleJob stepCtx) throws EngineException {
        copyChainInputs(stepCtx, pipeline);
        run(stepCtx, pipeline);
    }

    private void run(SimpleJob job, Pipeline pipeline) throws EngineException {
        String executeCmd = getExecutionCommand(job, pipeline);
        try {
            reporter.reportInfo("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName());
            IOUtils.createFolder(job.getEnvironment().getOutputsDirectory());
            ProcessRunner.runOnSpecificFolder(executeCmd, reporter, workingDirectory + File.separatorChar + pipeline.getName());
            validateOutputs(job, pipeline.getName());
        } catch (EngineException e) {
            logger.error("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            updateState(pipeline, job, e, StateEnum.FAILED);
            throw new EngineException("Error executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
        }
    }


    private void copyChainInputs(SimpleJob job, Pipeline pipeline) throws EngineException {
        String jobId = job.getId();
        String destDir = pipeline.getEnvironment().getWorkDirectory() + fileSeparator;

        for (Input inputCtx : job.getInputs()) {
            if (!inputCtx.getOriginStep().equals(jobId)) {
                Job chainStep = pipeline.getJobById(inputCtx.getOriginStep());
                String outDir = chainStep.getEnvironment().getOutputsDirectory() + fileSeparator;
                Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
                List<String> usedBy = outCtx.getUsedBy();

                String destDir1 = destDir + jobId + File.separatorChar;
                if (usedBy != null) {
                    for (String dependent : usedBy) {
                        Output outputCtx = chainStep.getOutputById(dependent);
                        String value = outputCtx.getValue().toString();
                        copyInput(destDir1, outDir, value);
                    }
                } else {
                    String value = outCtx.getValue().toString();
                    copyInput(destDir1, outDir, value);
                }
            }
        }
    }

    private void copyInput(String destDir, String outDir, String value) throws EngineException {
        try {
            String source = outDir + value;
            source = source.replace(fileSeparator, File.separatorChar + "");
            destDir = destDir.replace(fileSeparator, File.separatorChar + "");
            IOUtils.copyFile(source, destDir);
        } catch (IOException e) {
            throw new EngineException("Error copying input: " + value , e);
        }
    }

    private String getExecutionCommand(SimpleJob job, Pipeline pipeline) throws EngineException {
        try {
            String cmdBuilded = job.getCommandBuilder().build(pipeline, job, fileSeparator, job.getExecutionContext().getConfig());
            StringBuilder command = new StringBuilder("vagrant ssh ");
            command.append(TAG)
                    .append(pipeline.getName())
                    .append(" -c \"")
                    .append(cmdBuilded)
                    .append("\"");
            return command.toString().replace(WORK_DIRECTORY, BASE_DIRECTORY);
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new EngineException("Error when building step", e);
        }
    }

    private void validateOutputs(SimpleJob job, String pipelineName) throws EngineException {
        String basePath = WORK_DIRECTORY + File.separatorChar + pipelineName + File.separatorChar + job.getId();
        for (Output outCtx : job.getOutputs()) {
            String type = outCtx.getType();
            if (type.contains("ile") || type.contains("irectory")) {
                String out = outCtx.getValue().toString();
                try {
                    IOUtils.findFiles(basePath, out);
                } catch (IOException e) {
                    throw new EngineException("Output " + outCtx.getName() +
                            " not found. Error running job " + job.getId(), e);
                }
            }
        }
    }

    private void copyInput(Job job, Input input) throws EngineException {
        String type = input.getType();
        if (type.equalsIgnoreCase("file") || type.equalsIgnoreCase("directory") || type.equalsIgnoreCase("file[]")) {
            if (input.getChainOutput() == null || input.getChainOutput().isEmpty()) {
                String value = input.getValue();
                if (type.equalsIgnoreCase("directory")) {
                    IOUtils.createFolder(job.getEnvironment().getWorkDirectory() + File.separatorChar + value);
                } else {
                    String fileName = value.substring(value.lastIndexOf(File.separatorChar) + 1);
                    if (type.contains("[]") || job.getSpread() != null) {
                        value = value.replace("[", "");
                        value = value.replace("]", "");
                        value = value.replace(" ", "");
                        String[] values = value.split(",");
                        for (String val : values)
                            copyInput(val, job);
                    } else  {
                        copyInput(value, job);
                    }
                    input.setValue(fileName);
                }
            }
        }
    }

    private void copyInput(String input, Job stepCtx) throws EngineException {
        String inputName = input.substring(input.lastIndexOf(File.separatorChar) - 1);
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + inputName;
        try {
            IOUtils.copyFile(input, destInput);
        } catch (IOException e) {
            throw new EngineException("Error copying input file " + inputName, e);
        }
    }
}
