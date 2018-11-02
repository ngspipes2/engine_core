package pt.isel.ngspipes.engine_core.implementations;

import pt.isel.ngspipes.engine_core.entities.Environment;
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
import pt.isel.ngspipes.engine_core.utils.IOUtils;
import pt.isel.ngspipes.engine_core.utils.ProcessRunner;
import pt.isel.ngspipes.engine_core.utils.SpreadCombiner;
import pt.isel.ngspipes.engine_core.utils.ValidateUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class EngineLocalDefault extends Engine {

    private static final String RUN_CMD = "%1$s";
    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
                                                    File.separatorChar + "Engine";
    private static final String TAG = "LocalEngine";

    private final Map<String, Map<Job, BasicTask<Void>>> TASKS_BY_EXEC_ID = new HashMap<>();
    private final ConsoleReporter reporter = new ConsoleReporter();


    public EngineLocalDefault(String workingDirectory) {
        super(workingDirectory);
    }

    public EngineLocalDefault() { super(WORK_DIRECTORY); }

    @Override
    protected void stage(Pipeline pipeline) throws EngineException {
        copyPipelineInputs(pipeline);
        schedulePipeline(pipeline);
        pipeline.getState().setState(StateEnum.SCHEDULE);
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



    private void schedulePipeline(Pipeline pipeline) {
        logger.trace(TAG + ":: Scheduling pipeline " + pipeline.getName() + " " + pipeline.getName());
        String executionId = pipeline.getName();
        TASKS_BY_EXEC_ID.put(executionId, new HashMap<>());
        Collection<ExecutionNode> executionGraph = pipeline.getGraph();
        run(pipeline, executionGraph);
    }

    private void updatePipelineState(String executionId, ExecutionState state) {
        pipelines.get(executionId).setState(state);
    }

    private void run(Pipeline pipeline, Collection<ExecutionNode> executionGraph) {
        Map<Job, BasicTask<Void>> taskMap = getTasks(executionGraph, pipeline, new HashMap<>());
        TASKS_BY_EXEC_ID.put(pipeline.getName(), taskMap);
        pipeline.getState().setState(StateEnum.RUNNING);
        scheduleFinishPipelineTask(pipeline, taskMap);
        scheduleChildTasks(pipeline, taskMap);
        scheduleParentsTasks(executionGraph, pipeline.getName(), taskMap);
    }

    private void scheduleFinishPipelineTask(Pipeline pipeline, Map<Job, BasicTask<Void>> taskMap) {
        BasicTask<Void> task = (BasicTask<Void>) TaskFactory.createTask(() -> {
            try {
                pipeline.getState().setState(StateEnum.SUCCESS);
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
        Collection<Task<Void>> parentsTasks = getParentsTasks(pipeline, job.getParents(), taskMap);
        Task<Collection<Void>> tasks = TaskFactory.whenAllTasks(parentsTasks);
        tasks.then(taskMap.get(job));
    }

    private Collection<Task<Void>> getParentsTasks(Pipeline pipeline, Collection<String> parents, Map<Job, BasicTask<Void>> taskMap) {
        Collection<Task<Void>> parentsTasks = new LinkedList<>();

        for (String parent : parents)
            parentsTasks.add(taskMap.get(pipeline.getJobById(parent)));

        return parentsTasks;
    }

    private Map<Job, BasicTask<Void>> getTasks(Collection<ExecutionNode> executionGraph, Pipeline pipeline, Map<Job, BasicTask<Void>> tasks) {

        for (ExecutionNode node : executionGraph) {
            Job job = node.getJob();
            BasicTask<Void> task = (BasicTask<Void>) TaskFactory.createTask(() -> {
                try {
                    runTask(job, pipeline);
                } catch (EngineException e) {
                    updateState(pipeline, job, e);
                }
            });
            tasks.put(job, task);
            TASKS_BY_EXEC_ID.get(pipeline.getName()).put(job, task);
            getTasks(node.getChilds(), pipeline, tasks);
        }

        return tasks;
    }

    private void copyPipelineInputs(Pipeline pipeline) throws EngineException {
        logger.trace(TAG + ":: Copying pipeline " + pipeline.getName() + " "
                + pipeline.getName() + " inputs.");

        for (Job job : pipeline.getJobs()) {
            copyInputs(job, job.getInputs());
        }
    }

    private void copyInputs(Job job, List<Input> inputs) throws EngineException {
        for (Input input : inputs) {
            copyInput(job, input);
            copyInputs(job, input.getSubInputs());
        }
    }

    private void copyInput(Job step, Input input) throws EngineException {
        if (input.getType().equalsIgnoreCase("file") || input.getType().equalsIgnoreCase("directory")) {
            if (input.getChainOutput() == null || input.getChainOutput().isEmpty()) {
                 copyInput(input.getValue(), step);
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
            try {
                logger.trace(TAG + ":: Executing step " + job.getId());
                task.get(job).run();
//                runTask(job, pipelines.get(executionId));
            } catch (Exception e) {
                logger.error(TAG + ":: Executing step " + job.getId(), e);
                throw new EngineException("Error executing step: " + job.getId(), e);
            }
        }
    }

    private void updateState(Pipeline pipeline, Job job, EngineException e) {
        ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
        job.getState().setState(state.getState());
        pipeline.setState(state);
    }

    private void runTask(Job job, Pipeline pipeline) throws EngineException {
        ValidateUtils.validatePipelineState(pipeline);
        ValidateUtils.validateResources(job, pipeline);
        job.getState().setState(StateEnum.RUNNING);

        if (job instanceof ComposeJob) {
            throw new NotImplementedException();
//            executeSubPipeline(job.getId(), pipeline);
//            return;
        }

        SimpleJob spreadJob = (SimpleJob) job;
        if (spreadJob.getSpread() != null) {
            runSpreadStep(pipeline, spreadJob);
        } else {
            execute(pipeline, spreadJob);
        }
        job.getState().setState(StateEnum.SUCCESS);
    }

    private void execute(Pipeline pipeline, SimpleJob stepCtx) throws EngineException {
        copyChainInputs(stepCtx, pipeline);
        run(stepCtx, pipeline);
    }

    private void runSpreadStep(Pipeline pipeline, SimpleJob job) throws EngineException {
        Spread spread = job.getSpread();
        Map<String, Collection<String>> valuesOfInputsToSpread = getInputValuesToSpread(job, pipeline);
        SpreadCombiner.getInputsCombination(spread.getStrategy(), valuesOfInputsToSpread);

        int idx = 0;
        int len = getInputValuesLength(valuesOfInputsToSpread);

        while (idx < len) {
            SimpleJob jobToSpread = getSpreadJob(job, valuesOfInputsToSpread, idx);
            try {
                execute(pipeline, jobToSpread);
            } catch (EngineException e) {
                updateState(pipeline, jobToSpread, e);
            }
            idx++;
        }
    }

    private int getInputValuesLength(Map<String, Collection<String>> valuesOfInputsToSpread) {
        if (valuesOfInputsToSpread.values().iterator().hasNext())
            return valuesOfInputsToSpread.values().iterator().next().size();
        return 0;
    }

    private SimpleJob getSpreadJob(SimpleJob stepCtx, Map<String, Collection<String>> inputs, int idx) {
        ExecutionContext executionContext = stepCtx.getExecutionContext();

        String id = stepCtx.getId() + idx;
        Environment env = copyEnvironment(stepCtx, id);
        SimpleJob stepContext = new SimpleJob(id, env, stepCtx.getCommand(), executionContext);
        return stepContext;
    }

    private Environment copyEnvironment(SimpleJob stepCtx, String stepId) {
        Environment environment = new Environment();

        Environment baseEnvironment = stepCtx.getEnvironment();
        environment.setDisk(baseEnvironment.getDisk());
        environment.setMemory(baseEnvironment.getMemory());
        environment.setCpu(baseEnvironment.getCpu());
        environment.setWorkDirectory(baseEnvironment.getWorkDirectory() + stepId);

        return environment;
    }

    private Map<String, Collection<String>> getInputValuesToSpread(SimpleJob stepCtx, Pipeline pipeline) throws EngineException {
        Map<String, Collection<String>> valuesOfInputsToSpread = new HashMap<>();
        Collection<String> inputsToSpread = stepCtx.getSpread().getInputsToSpread();

        for (Input input : stepCtx.getInputs()) {
            if (inputsToSpread.contains(input.getName())) {
                valuesOfInputsToSpread.put(input.getName(), getValues(input));
            }
        }

        return valuesOfInputsToSpread;
    }

    private Collection<String> getValues(Input input) {
        String inputValue = input.getValue();
        inputValue = inputValue.replace("[", "");
        inputValue = inputValue.replace("]", "");

        String[] split = inputValue.split(",");
        Collection<String> inputsValues = new LinkedList<>();

        for (String str : split)
            inputsValues.add(str.trim());

        return inputsValues;
    }

    private void run(SimpleJob stepCtx, Pipeline pipeline) throws EngineException {
        String executeCmd = getExecutionCommand(stepCtx, pipeline);
        try {
            reporter.reportInfo("Executing step: " + stepCtx.getId()
                    + " from pipeline: " + pipeline.getName());
            IOUtils.createFolder(stepCtx.getEnvironment().getOutputsDirectory());
            ProcessRunner.run(executeCmd, stepCtx.getEnvironment().getWorkDirectory(), reporter);
            validateOutputs(stepCtx);
        } catch (EngineException e) {
            logger.error("Executing step: " + stepCtx.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            throw new EngineException("Error executing step: " + stepCtx.getId()
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

    private String  getExecutionCommand(SimpleJob stepCtx, Pipeline pipeline) throws EngineException {
        try {
            String command = stepCtx.getCommandBuilder().build(pipeline, stepCtx.getId());
            return String.format(RUN_CMD, command);
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + stepCtx.getId(), e);
            throw new EngineException("Error when building step", e);
        }
    }

    private void copyChainInputs(SimpleJob stepCtx, Pipeline pipeline) throws EngineException {

        String destDir = stepCtx.getEnvironment().getWorkDirectory() + File.separatorChar;

        for (Input inputCtx : stepCtx.getInputs()) {
            if (!inputCtx.getOriginStep().equals(stepCtx.getId())) {
                Job chainStep = pipeline.getJobById(inputCtx.getOriginStep());
                String outDir = chainStep.getEnvironment().getOutputsDirectory() + File.separatorChar;
                Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
                List<String> usedBy = outCtx.getUsedBy();

                if (usedBy != null) {
                    for (String dependet : usedBy) {
                        Output outputCtx = chainStep.getOutputById(dependet);
                        String value = outputCtx.getValue().toString();
                        copyChainInput(outDir, destDir, outputCtx, value);
                    }
                } else {
                    String value = outCtx.getValue().toString();
                    copyChainInput(outDir, destDir, outCtx, value);
                }
            }
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
