package pt.isel.ngspipes.engine_core.implementations;

import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.StateEnum;
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
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class EngineLocalDefault extends Engine {

    private static final String RUN_CMD = "%1$s";
    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
                                                    File.separatorChar + "Engine";
    private static final String TAG = "LocalEngine";

    private final Map<String, Map<Job, BasicTask<Void>>> TASKS_BY_EXEC_ID = new HashMap<>();
    private final ConsoleReporter reporter = new ConsoleReporter();


    public EngineLocalDefault(String workingDirectory) {
        super(workingDirectory, TAG);
    }

    public EngineLocalDefault() { super(WORK_DIRECTORY, TAG); }

    @Override
    public void run(Pipeline pipeline, Collection<ExecutionNode> graph) throws EngineException {
        if (!TASKS_BY_EXEC_ID.containsKey(pipeline.getName())) {
            TASKS_BY_EXEC_ID.put(pipeline.getName(), new HashMap<>());
            pipeline.getState().setState(StateEnum.RUNNING);
        }
        Map<Job, BasicTask<Void>> taskMap = getTasks(graph, pipeline, new HashMap<>());
//        scheduleFinishPipelineTask(pipeline, taskMap);
        scheduleChildTasks(pipeline, taskMap);
        scheduleParentsTasks(graph, pipeline.getName(), taskMap);
    }

    @Override
    public void copyPipelineInputs(Pipeline pipeline) throws EngineException {
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
        List<String> fileNamesByPattern = IOUtils.getFileNamesByPattern(outputsDirectory + File.separatorChar + spreadId, pattern);
        return new LinkedList<>(fileNamesByPattern);
    }

    @Override
    public boolean stop(String executionId) {
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


    private void updatePipelineState(String executionId, ExecutionState state) {
        pipelines.get(executionId).setState(state);
    }

    private void scheduleFinishPipelineTask(Pipeline pipeline, Map<Job, BasicTask<Void>> taskMap) {
        BasicTask<Void> task = (BasicTask<Void>) TaskFactory.createTask(() -> {
            try {
                pipeline.getState().setState(StateEnum.SUCCESS);
                reporter.reportInfo("Pipeline Finished");
                TaskFactory.dispose();
            } catch (Exception e) {

            }
        });
        Task<Collection<Void>> tasks = TaskFactory.whenAllTasks(new ArrayList<>(taskMap.values()));
        tasks.then(task);
    }

    private void scheduleChildTasks(Pipeline pipeline, Map<Job, BasicTask<Void>> taskMap) {
        for (Map.Entry<Job, BasicTask<Void>> entry : taskMap.entrySet()) {
            if (!entry.getKey().getParents().isEmpty()) {
                runWhenAll(pipeline, entry.getKey(), taskMap);
            }
        }
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

    private void copyInputs(Job job, List<Input> inputs) throws EngineException {
        for (Input input : inputs) {
            copyInput(job, input);
            copyInputs(job, input.getSubInputs());
        }
    }

    private void copyInput(Job job, Input input) throws EngineException {
        String type = input.getType();
        if (type.equalsIgnoreCase("file") || type.equalsIgnoreCase("directory") || type.equalsIgnoreCase("file[]")) {
            if (input.getChainOutput() == null || input.getChainOutput().isEmpty()) {
                String value = input.getValue();
                if (type.contains("[]") || job.getSpread() != null) {
                    value = value.replace("[", "");
                    value = value.replace("]", "");
                    value = value.replace(" ", "");
                    String[] values = value.split(",");
                    for (String val : values)
                        copyInput(val, job);
                } else
                    copyInput(value, job);
            }
        }
    }

    private void copyInput(String input, Job stepCtx) throws EngineException {
        String inputName = input.substring(input.lastIndexOf(File.separatorChar));
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + inputName;
        try {
            IOUtils.copyFile(input, destInput);
        } catch (IOException e) {
            throw new EngineException("Error copying input file " + inputName, e);
        }
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

    private void updateState(Pipeline pipeline, Job job, EngineException e, StateEnum state) {
        ExecutionState newState = new ExecutionState(state, e);
        job.getState().setState(newState.getState());
        if (e != null)
            pipeline.setState(newState);
    }

    private void runTask(Job job, Pipeline pipeline) throws EngineException {
        ValidateUtils.validatePipelineState(pipeline);
        ValidateUtils.validateResources(job, pipeline);
        job.getState().setState(StateEnum.RUNNING);

        if (job instanceof ComposeJob) {
            throw new NotImplementedException();
        }

        SimpleJob simpleJob = (SimpleJob) job;
        if (simpleJob.getSpread() != null) {
            LinkedList<ExecutionNode> graph = new LinkedList<>();
            SpreadJobExpander.expandSpreadJob(pipeline, simpleJob, graph, this::getOutputValues);
            run(pipeline, graph);
            /*
            List<Job> expandedJobs = new LinkedList<>();
            getExpandedJobs(graph, expandedJobs);
            List<Task<Void>> childs = expandedJobs.stream().map((expendedJob) ->
                TASKS_BY_EXEC_ID.get(pipeline.getName()).get(expendedJob)
            ).collect(Collectors.toList());

            try {
                TaskFactory.whenAllTasks(childs).finishedEvent.await();
            } catch (InterruptedException e) {
                throw new EngineException("Error waiting for dependent jobs", e);
            }*/
        } else {
            execute(pipeline, simpleJob);
        }
        updateState(pipeline, job, null, StateEnum.SUCCESS);
        if (job.isInconclusive()) {
            Collection<ExecutionNode> childGraph = TopologicSorter.parallelSort(pipeline, job);
            run(pipeline, childGraph);
        }
    }

    private void getExpandedJobs(List<ExecutionNode> graph, List<Job> expandedJobs) {
        for (ExecutionNode node : graph) {
            expandedJobs.add(node.getJob());
            getExpandedJobs(node.getChilds(), expandedJobs);
        }
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
            ProcessRunner.run(executeCmd, job.getEnvironment().getWorkDirectory(), reporter);
            validateOutputs(job);
        } catch (EngineException e) {
            logger.error("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            updateState(pipeline, job, e, StateEnum.SUCCESS);
            throw new EngineException("Error executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
        }
    }

    private void validateOutputs(SimpleJob stepCtx) throws EngineException {
        for (Output outCtx : stepCtx.getOutputs()) {
            String type = outCtx.getType();
            if (type.equalsIgnoreCase("file") || type.equalsIgnoreCase("directory")) {
                String out = stepCtx.getEnvironment().getOutputsDirectory() + File.separatorChar + outCtx.getValue();
                try {
                    IOUtils.verifyFile(out);
                } catch (IOException e) {
                    throw new EngineException("Output " + outCtx.getName() +
                                              " not found. Error running job " + stepCtx.getId(), e);
                }
            }
        }
    }

    private String  getExecutionCommand(SimpleJob job, Pipeline pipeline) throws EngineException {
        try {
            String command = job.getCommandBuilder().build(pipeline, job);
            return String.format(RUN_CMD, command);
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new EngineException("Error when building step", e);
        }
    }

    private void copyChainInputs(SimpleJob job, Pipeline pipeline) throws EngineException {

        String destDir = job.getEnvironment().getWorkDirectory() + File.separatorChar;

        for (Input input : job.getInputs()) {
            String originStep = input.getOriginStep();
            if (!originStep.equals(job.getId())) {
                String spreadId = originStep + "_" + originStep;
                String type = input.getType();
                if (job.getId().startsWith(spreadId)) {
                    if (SpreadJobExpander.isResourceType(type))
                        copySpreadInput(destDir, input, job.getId());
                } else {
                    Job chainJob = pipeline.getJobById(originStep);

                    String outDir = chainJob.getEnvironment().getOutputsDirectory() + File.separatorChar;
                    Output outCtx = chainJob.getOutputById(input.getChainOutput());
                    List<String> usedBy = outCtx.getUsedBy();

                    if (chainJob.getSpread() != null && job.getSpread() == null) {
                        if (!usedBy.isEmpty())
                            outCtx = chainJob.getOutputById(usedBy.get(0));

                        int beginIdx = spreadId.lastIndexOf(job.getId()) + job.getId().length();
                        int idx = Integer.parseInt(job.getId().substring(beginIdx));
                        String spreadJobId = spreadId + idx;
                        List<String> outputValuesFromSpreadJob = getOutputValuesFromSpreadJob(outCtx.getName(), chainJob, spreadJobId);
                        outDir = outDir + spreadJobId + File.separatorChar;
                        for (String value : outputValuesFromSpreadJob) {
                            copyChainInput(outDir, destDir, outCtx, value);
                        }
                    } else { // OTHER CASES
                        copyFromSingleJob(destDir, chainJob, outDir, outCtx, usedBy);
                    }
                }
            }
        }
    }

    private void copySpreadInput(String dest, Input input, String jobId) throws EngineException {
        String source = dest.substring(0, dest.indexOf(jobId));
        String inValue = input.getValue();
        int begin = inValue.lastIndexOf(File.separatorChar);
        String value = inValue.substring(begin + 1);
        String valStr = source + value;
        if (input.getType().equalsIgnoreCase("directory")) {
            copyDirectory(valStr, dest + value);
        } else if (input.getType().contains("file")) {
            copyFiles(valStr, dest + value);
        }
    }

    private void copyFromSingleJob(String destDir, Job chainJob, String outDir, Output outCtx, List<String> usedBy) throws EngineException {
        if (usedBy != null) {
            for (String dependet : usedBy) {
                Output outputCtx = chainJob.getOutputById(dependet);
                String value = outputCtx.getValue().toString();
                copyChainInput(outDir, destDir, outputCtx, value);
            }
        } else {
            String value = outCtx.getValue().toString();
            copyChainInput(outDir, destDir, outCtx, value);
        }
    }

    private void copyChainInput(String source, String dest, Output outCtx, String value) throws EngineException {
        String valStr = source + value;
        int begin = valStr.lastIndexOf(File.separatorChar);
        String inputName = valStr.substring(begin + 1);
        if (outCtx.getType().equalsIgnoreCase("directory")) {
            copyDirectory(valStr, dest + inputName);
        } else if (outCtx.getType().contains("file")) {
            copyFiles(valStr, dest + inputName);
        }
    }

    private void copyDirectory(String source, String dest) throws EngineException {
        int begin = source.lastIndexOf(File.separatorChar);
        String inputName = source.substring(begin + 1);
        try {
            IOUtils.copyDirectory(source, dest);
        } catch (IOException e) {
            throw new EngineException("Error copying chain input " + inputName, e);
        }
    }

    private void copyFiles(String source, String dest) throws EngineException {
        int begin = source.lastIndexOf(File.separatorChar);
        String inputName = source.substring(begin + 1);
        try {
            IOUtils.copyFile(source, dest);
        } catch (IOException e) {
            throw new EngineException("Error copying chain input " + inputName, e);
        }
    }

}
