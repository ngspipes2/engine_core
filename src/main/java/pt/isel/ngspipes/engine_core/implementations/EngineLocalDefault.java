package pt.isel.ngspipes.engine_core.implementations;

import pt.isel.ngspipes.engine_core.commandBuilders.ICommandBuilder;
import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.StateEnum;
import pt.isel.ngspipes.engine_core.entities.contexts.ComposeStepContext;
import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.entities.contexts.SimpleStepContext;
import pt.isel.ngspipes.engine_core.entities.contexts.StepContext;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.executionReporter.ConsoleReporter;
import pt.isel.ngspipes.engine_core.tasks.BasicTask;
import pt.isel.ngspipes.engine_core.tasks.Task;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.*;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.StepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.SimpleInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.ISpreadDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IExecutionContextDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IOutputDescriptor;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class EngineLocalDefault extends Engine {

    private static final String RUN_CMD = "%1$s";
    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
                                                    File.separatorChar + "Engine";
    private static final String TAG = "LocalEngine";


    private final Map<String, Collection<BasicTask<Void>>> TASKS_BY_EXEC_ID = new HashMap<>();
    private final ConsoleReporter reporter = new ConsoleReporter();


    public EngineLocalDefault(String workingDirectory) {
        super(workingDirectory);
    }

    public EngineLocalDefault() { super(WORK_DIRECTORY); }

    @Override
    protected void stage(PipelineContext pipeline) throws EngineException {
            copyPipelineInputs(pipeline);
            schedulePipeline(pipeline);
            pipeline.getState().setState(StateEnum.SCHEDULE);
    }



    private void schedulePipeline(PipelineContext pipeline) {
        logger.trace(TAG + ":: Scheduling pipeline " + pipeline.getExecutionId() + " " + pipeline.getPipelineName());
        String executionId = pipeline.getExecutionId();
        TASKS_BY_EXEC_ID.put(executionId, new LinkedList<>());
        Collection<ExecutionNode> executionGraph = pipeline.getPipelineGraph();
        run(pipeline, executionId, executionGraph);
    }

    @Override
    public boolean stop(String executionId) {
        boolean stopped = true;
        for (BasicTask<Void> task : TASKS_BY_EXEC_ID.get(executionId)){
            task.cancel();
            try {
                stopped = stopped && task.cancelledEvent.await(200);
            } catch (InterruptedException e) {
                ExecutionState state = new ExecutionState();
                state.setState(StateEnum.STOPPED);
                updatePipelineState(executionId, state);
            }
        }
        return stopped;
    }



    private void updatePipelineState(String executionId, ExecutionState state) {
        pipelines.get(executionId).setState(state);
    }

    private void run(PipelineContext pipeline, String executionId, Collection<ExecutionNode> executionGraph) {
        Map<StepContext, BasicTask<Void>> taskMap = getTasks(executionGraph, pipeline, new HashMap<>());
        scheduleChildTasks(taskMap);
        scheduleParentsTasks(executionGraph, executionId, taskMap);
    }

    private void copyPipelineInputs(PipelineContext pipeline) throws EngineException {
        logger.trace(TAG + ":: Copying pipeline " + pipeline.getExecutionId() + " "
                + pipeline.getPipelineName() + " inputs.");

        for (Map.Entry<String, Collection<String>> entry : pipeline.getPipelineEnvironment().getInputs().entrySet()) {
            Collection<String> inputs = entry.getValue();
            StepContext stepCtx = pipeline.getStepsContexts().get(entry.getKey());
            for (String input : inputs)
                copyInput(input, stepCtx);
        }
    }

    private void copyInput(String input, StepContext stepCtx) throws EngineException {
        String inputName = input.substring(input.lastIndexOf(File.separatorChar));
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + inputName;
        try {
            IOUtils.copyFile(input, destInput);
        } catch (IOException e) {
            throw new EngineException("Error copying input file " + inputName, e);
        }
    }

    private void scheduleParentsTasks(Collection<ExecutionNode> executionGraph, String executionId,
                                      Map<StepContext, BasicTask<Void>> taskMap) {
        try {
            executeParents(executionGraph, taskMap);
        } catch (EngineException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(executionId, state);
        }
    }

    private void executeParents(Collection<ExecutionNode> executionGraph, Map<StepContext, BasicTask<Void>> taskMap)
                                throws EngineException {
        for (ExecutionNode parentNode : executionGraph) {
            StepContext stepContext = parentNode.getStepContext();
            try {
                logger.trace(TAG + ":: Executing step " + stepContext.getId());
                taskMap.get(stepContext).run();
            } catch (Exception e) {
                logger.error(TAG + ":: Executing step " + stepContext.getId(), e);
                throw new EngineException("Error executing step: " + stepContext.getId(), e);
            }
        }
    }

    private void scheduleChildTasks(Map<StepContext, BasicTask<Void>> taskMap) {
        for (Map.Entry<StepContext, BasicTask<Void>> entry : taskMap.entrySet()) {
            if (!entry.getKey().getParents().isEmpty()) {
                runWhenAll(entry.getKey(), taskMap);
            }
        }
    }

    private void runWhenAll(StepContext stepContext, Map<StepContext, BasicTask<Void>> taskMap) {
        Collection<Task<Void>> parentsTasks = getParentsTasks(stepContext.getParents(), taskMap);
        Task<Collection<Void>> tasks = TaskFactory.whenAllTasks(parentsTasks);
        tasks.then(taskMap.get(stepContext));

    }

    private Collection<Task<Void>> getParentsTasks(Collection<StepContext> parents, Map<StepContext, BasicTask<Void>> taskMap) {
        Collection<Task<Void>> parentsTasks = new LinkedList<>();

        for (StepContext parent : parents)
            parentsTasks.add(taskMap.get(parent));

        return parentsTasks;
    }

    private Map<StepContext, BasicTask<Void>> getTasks(Collection<ExecutionNode> executionGraph, PipelineContext pipeline,
                                                        Map<StepContext, BasicTask<Void>> taskMap) {

        String executionId = pipeline.getExecutionId();

        for (ExecutionNode node : executionGraph) {
            StepContext stepContext = node.getStepContext();
            BasicTask<Void> task = (BasicTask<Void>) TaskFactory.createTask(() -> {
                try {
                    runTask(stepContext, pipeline);
                } catch (EngineException e) {
                    updateState(pipeline, stepContext, e);
                }
            });
            taskMap.put(stepContext, task);
            TASKS_BY_EXEC_ID.get(executionId).add(task);
            getTasks(node.getChilds(), pipeline, taskMap);
        }

        return taskMap;
    }

    private void updateState(PipelineContext pipeline, StepContext stepContext, EngineException e) {
        ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
        stepContext.getState().setState(state.getState());
        pipeline.setState(state);
    }

    private void runTask(StepContext stepContext, PipelineContext pipeline) throws EngineException {
        ValidateUtils.validatePipelineState(pipeline);
        ValidateUtils.validateResources(stepContext, pipeline);

        if (stepContext instanceof ComposeStepContext) {
            executeSubPipeline(stepContext.getId(), pipeline);
            return;
        }

        SimpleStepContext stepCtx = (SimpleStepContext) stepContext;
        if (stepContext.getStep().getSpread() != null) {
            runSpreadStep(pipeline, stepCtx);
        } else {
            run(stepCtx, pipeline);
        }
    }

    private void runSpreadStep(PipelineContext pipeline, SimpleStepContext stepCtx) throws EngineException {
        ISpreadDescriptor spread = stepCtx.getStep().getSpread();
        Map<String, Collection<String>> valuesOfInputsToSpread = getInputValuesToSpread(stepCtx, pipeline);
        SpreadCombiner.getInputsCombination(spread.getStrategy(), valuesOfInputsToSpread);

        int idx = 0;
        int len = getInputValuesLength(valuesOfInputsToSpread);

        while (idx < len) {
            SimpleStepContext stepContext = getSpreadStepContext(stepCtx, valuesOfInputsToSpread, idx);
            createAndExecuteTask(pipeline, stepContext);
            idx++;
        }
    }

    private int getInputValuesLength(Map<String, Collection<String>> valuesOfInputsToSpread) {
        if (valuesOfInputsToSpread.values().iterator().hasNext())
            return valuesOfInputsToSpread.values().iterator().next().size();
        return 0;
    }

    private void createAndExecuteTask(PipelineContext pipeline, SimpleStepContext stepContext) {
        TaskFactory.createAndExecuteTask(() -> {
            try {
                run(stepContext, pipeline);
            } catch (EngineException e) {
                updateState(pipeline, stepContext, e);
            }
        });
    }

    private SimpleStepContext getSpreadStepContext(SimpleStepContext stepCtx, Map<String, Collection<String>> inputs, int idx) {
        ICommandDescriptor commandDescriptor = stepCtx.getCommandDescriptor();
        ICommandBuilder commandBuilder = stepCtx.getCommandBuilder();
        IExecutionContextDescriptor executionContextDescriptor = stepCtx.getExecutionContextDescriptor();

        String id = stepCtx.getId() + idx;
        Environment env = copyEnvironment(stepCtx, id);
        IStepDescriptor step = copyStepDescriptor(stepCtx, id, inputs, idx);
        return new SimpleStepContext(id, env, step, commandDescriptor, commandBuilder, executionContextDescriptor);
    }

    private Map<String, String> getStepContextInputs(Map<String, Collection<String>> valuesOfInputsToSpread, int idx) {
        Map<String, String> inputs = new HashMap<>();
        for (Map.Entry<String, Collection<String>> entry : valuesOfInputsToSpread.entrySet()) {
            inputs.put(entry.getKey(), getValueById(entry.getValue(), idx));
        }
        return inputs;
    }

    private String getValueById(Collection<String> values, int idx) {
        return (String) values.toArray()[idx];
    }

    private IStepDescriptor copyStepDescriptor(SimpleStepContext stepCtx,
                                               String stepId, Map<String, Collection<String>> inputs, int idx) {
        StepDescriptor stepDescriptor = new StepDescriptor();
        IStepDescriptor baseStep = stepCtx.getStep();

        stepDescriptor.setExec(baseStep.getExec());
        stepDescriptor.setExecutionContext(baseStep.getExecutionContext());
        stepDescriptor.setId(stepId);
        Map<String, String> stepInputs = getStepContextInputs(inputs, idx);
        stepDescriptor.setInputs(getInputDescriptors(stepInputs, baseStep));

        return stepDescriptor;
    }

    private Collection<IInputDescriptor> getInputDescriptors(Map<String, String> inputs, IStepDescriptor step) {
        Collection<IInputDescriptor> stepInputs = step.getInputs();
        Collection<IInputDescriptor> inputsToSpread = new LinkedList<>();
        for (IInputDescriptor input : stepInputs) {
            String inputName = input.getInputName();
            if (inputs.containsKey(inputName)) {
                inputsToSpread.add(new SimpleInputDescriptor(inputName, inputs.get(inputName)));
            } else
                inputsToSpread.add(input);
        }
        return inputsToSpread;
    }

    private Environment copyEnvironment(SimpleStepContext stepCtx, String stepId) {
        Environment environment = new Environment();

        Environment baseEnvironment = stepCtx.getEnvironment();
        environment.setDisk(baseEnvironment.getDisk());
        environment.setMemory(baseEnvironment.getMemory());
        environment.setCpu(baseEnvironment.getCpu());
        environment.setWorkDirectory(baseEnvironment.getWorkDirectory() + stepId);

        return environment;
    }

    private Map<String, Collection<String>> getInputValuesToSpread(SimpleStepContext stepCtx, PipelineContext pipeline) throws EngineException {
        Map<String, Collection<String>> valuesOfInputsToSpread = new HashMap<>();
        Collection<String> inputsToSpread = stepCtx.getStep().getSpread().getInputsToSpread();

        for (IInputDescriptor input : stepCtx.getStep().getInputs()) {
            if (inputsToSpread.contains(input.getInputName())) {
                valuesOfInputsToSpread.put(input.getInputName(), getValues(input, pipeline));
            }
        }

        return valuesOfInputsToSpread;
    }

    private Collection<String> getValues(IInputDescriptor input, PipelineContext pipeline) throws EngineException {
        String inputValue = ContextFactory.getInputValue(input, pipeline);
        inputValue = inputValue.replace("[", "");
        inputValue = inputValue.replace("]", "");

        String[] split = inputValue.split(",");
        Collection<String> inputsValues = new LinkedList<>();

        for (String str : split)
            inputsValues.add(str.trim());

        return inputsValues;
    }

    private void run(SimpleStepContext stepCtx, PipelineContext pipeline) throws EngineException {
        String executeCmd = getExecutionCommand(stepCtx, pipeline);
        try {
            reporter.reportInfo("Executing step: " + stepCtx.getId()
                    + " from pipeline: " + pipeline.getExecutionId());
            IOUtils.createFolder(stepCtx.getEnvironment().getOutputsDirectory());
            ProcessRunner.run(executeCmd, stepCtx.getEnvironment().getWorkDirectory(), reporter);
            setOutputs(stepCtx, pipeline);
        } catch (EngineException e) {
            logger.error("Executing step: " + stepCtx.getId()
                    + " from pipeline: " + pipeline.getExecutionId(), e);
            throw new EngineException("Error executing step: " + stepCtx.getId()
                    + " from pipeline: " + pipeline.getExecutionId(), e);
        }
    }

    private String getExecutionCommand(SimpleStepContext stepCtx, PipelineContext pipeline) throws EngineException {
        try {
            String command = stepCtx.getCommandBuilder().build(pipeline, stepCtx.getId());
            return String.format(RUN_CMD, command);
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + stepCtx.getId(), e);
            throw new EngineException("Error when building step", e);
        }
    }

    private void setOutputs(SimpleStepContext stepContext, PipelineContext pipeline) throws EngineException {
        Map<String, Object> outputs = new HashMap<>();

        for (IOutputDescriptor output : stepContext.getCommandDescriptor().getOutputs()) {
            String value = output.getValue();
            if(value.contains("$") && ValidateUtils.isOutputDependentInputSpecified(value, stepContext.getStep().getInputs()))
                outputs.put(output.getName(), ContextFactory.getOutputValue(output, stepContext, pipeline));
        }
        stepContext.setOutputs(outputs);
    }

}
