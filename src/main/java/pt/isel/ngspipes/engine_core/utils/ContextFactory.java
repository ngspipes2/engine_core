package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.commandBuilders.ICommandBuilder;
import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.PipelineEnvironment;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.CommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IPipelineExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IParameterInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ISimpleInputDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IExecutionContextDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IOutputDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IToolDescriptor;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.io.File;
import java.util.*;
import java.util.function.Function;

public class ContextFactory {

    public static PipelineContext create(String executionId, IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                                         Arguments arguments, String workingDirectory) throws EngineException {

        Map<String, StepContext> steps = getSteps(pipelineDescriptor, parameters, workingDirectory);
        PipelineEnvironment environment = getPipelineEnvironment(arguments, workingDirectory);
        return createPipelineContext(executionId, pipelineDescriptor, parameters, steps, environment);
    }

    public static PipelineContext create(String stepId, PipelineContext pipeline) throws EngineException {
        IStepDescriptor step = pipeline.getStepsContexts().get(stepId).getStep();
        IPipelineDescriptor pipelineDescriptor = getPipelineDescriptor(stepId, pipeline, step);
        String workingDirectory = pipeline.getPipelineEnvironment().getWorkDirectory() + "\\" + pipelineDescriptor.getName();
        Arguments arguments = getArgumentsFromPipeline(pipeline);
        return ContextFactory.create(pipeline.getExecutionId(), pipelineDescriptor, pipeline.getParameters(),
                arguments, workingDirectory);
    }

    public static InOutContext getOutputValue(IOutputDescriptor outputDescriptor, StepContext stepCtx,
                                              PipelineContext pipeline) throws EngineException {
        Object outputValue;
        if (outputDescriptor.getValue().contains("$"))
            outputValue = getDependentOutputValue(stepCtx, outputDescriptor, pipeline);
        else
            outputValue = getDependentOutputValue(stepCtx, outputDescriptor, pipeline);
        return new InOutContext(outputDescriptor.getName(), stepCtx.getId(), outputDescriptor.getType(), outputValue);
    }

    public static String getInputValue(IInputDescriptor inputDescriptor, PipelineContext pipeline) throws EngineException {
        if (inputDescriptor instanceof IParameterInputDescriptor) {
            return getParameterInputValue(inputDescriptor, pipeline.getParameters());
        } else if (inputDescriptor instanceof ISimpleInputDescriptor) {
            return getSimpleInputValue((ISimpleInputDescriptor) inputDescriptor);
        } else if (inputDescriptor instanceof IChainInputDescriptor) {
            return getChainInputValue((IChainInputDescriptor) inputDescriptor, pipeline);
        }

        return "";
    }

    static PipelineContext createSubPipeline(String stepId, PipelineContext pipeline) throws EngineException {
        ComposeStepContext stepCtx = (ComposeStepContext) pipeline.getStepsContexts().get(stepId);
        IPipelineDescriptor pipelineDescriptor = stepCtx.getPipelineDescriptor();
        Map<String, Object> parameters = pipeline.getParameters();
        Arguments arguments = getPipelineArguments(pipeline, stepCtx);

        return create(pipeline.getExecutionId(), pipelineDescriptor, parameters,
                        arguments, stepCtx.getEnvironment().getWorkDirectory());
    }

    private static Map<String, IPipelinesRepository> getPipelinesRepositories(Collection<IRepositoryDescriptor> repositories,
                                                                              Map<String, Object> parameters) throws EngineException {
        Map<String, IPipelinesRepository> pipelinesRepositoryMap = new HashMap<>();

        for (IRepositoryDescriptor repo : repositories) {
            if(pipelinesRepositoryMap.containsKey(repo.getId()))
                throw new EngineException("Repositories ids can be duplicated. Id: " + repo.getId() + " is duplicated.");
            if (repo instanceof IPipelineRepositoryDescriptor) {
                IPipelineRepositoryDescriptor pRepoDesc = (IPipelineRepositoryDescriptor) repo;
                IPipelinesRepository pipelinesRepo = RepositoryUtils.getPipelinesRepository(pRepoDesc, parameters);
                pipelinesRepositoryMap.put(repo.getId(), pipelinesRepo);
            }
        }

        return pipelinesRepositoryMap;
    }

    private static Map<String,IToolsRepository> getToolsRepositories(Collection<IRepositoryDescriptor> repositories, Map<String, Object> parameters) throws EngineException {
        Map<String, IToolsRepository> toolsRepositoryMap = new HashMap<>();

        for (IRepositoryDescriptor repo : repositories) {
            if(toolsRepositoryMap.containsKey(repo.getId()))
                throw new EngineException("Repositories ids can be duplicated. Id: " + repo.getId() + " is duplicated.");
            if (repo instanceof IToolRepositoryDescriptor) {
                IToolRepositoryDescriptor toolsRepoDesc = (IToolRepositoryDescriptor) repo;
                IToolsRepository toolsRepo = RepositoryUtils.getToolsRepository(toolsRepoDesc, parameters);
                toolsRepositoryMap.put(repo.getId(), toolsRepo);
            }
        }

        return toolsRepositoryMap;
    }

    private static Map<String, StepContext> getSteps(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters,
                                                     String workingDirectory) throws EngineException {

        Map<String, IToolsRepository> toolsRepos = getToolsRepositories(pipelineDesc.getRepositories(), parameters);
        Map<String, IPipelinesRepository> pipelinesRepos = getPipelinesRepositories(pipelineDesc.getRepositories(),
                parameters);
        Map<String, StepContext> steps = new HashMap<>();

        for (IStepDescriptor step : pipelineDesc.getSteps()) {
            String stepId = step.getId();

            if(steps.containsKey(stepId))
                throw new EngineException("Steps ids can be duplicated. Id: " + stepId + " is duplicated.");

            StepContext stepCtx = getStepContext(parameters, workingDirectory, toolsRepos, pipelinesRepos, step);
            steps.put(stepId, stepCtx);
        }

        return steps;
    }

    private static PipelineContext createPipelineContext(String executionId, IPipelineDescriptor pipelineDescriptor,
                                                         Map<String, Object> parameters, Map<String, StepContext> steps,
                                                         PipelineEnvironment environment) throws EngineException {
        PipelineContext pipelineContext = new PipelineContext(executionId, parameters, steps, pipelineDescriptor, environment);
        setInputs(pipelineDescriptor, pipelineContext);
        setStepsResources(pipelineContext, steps);
        return pipelineContext;
    }

    private static void setStepsResources(PipelineContext pipelineContext, Map<String, StepContext> steps) throws EngineException {

        for (Map.Entry<String, StepContext> step : steps.entrySet()) {
            StepContext stepCtx = step.getValue();
            Arguments arguments = getStepArguments(pipelineContext, stepCtx);
            stepCtx.getEnvironment().setMemory(arguments.mem);
            stepCtx.getEnvironment().setDisk(arguments.disk);
            stepCtx.getEnvironment().setCpu(arguments.cpus);
        }
    }

    private static Arguments getPipelineArguments(PipelineContext pipeline, ComposeStepContext stepCtx) throws EngineException {
        Arguments arguments = new Arguments();
        arguments.disk = getHighestValueFromPipeline(pipeline, stepCtx, ICommandDescriptor::getRecommendedDisk);
        arguments.cpus = getHighestValueFromPipeline(pipeline, stepCtx, ICommandDescriptor::getRecommendedMemory);
        arguments.mem = getHighestValueFromPipeline(pipeline, stepCtx, ICommandDescriptor::getRecommendedCpu);

        return arguments;
    }

    private static Arguments getArgumentsFromPipeline(PipelineContext pipeline) {
        Arguments arguments = new Arguments();
        arguments.disk = pipeline.getPipelineEnvironment().getDisk();
        arguments.mem = pipeline.getPipelineEnvironment().getMemory();
        arguments.cpus = pipeline.getPipelineEnvironment().getCpu();

        return arguments;
    }

    private static StepContext getStepContext(Map<String, Object> parameters, String workingDirectory,
                                              Map<String, IToolsRepository> toolsRepos,
                                              Map<String, IPipelinesRepository> pipelinesRepos,
                                              IStepDescriptor step) throws EngineException {
        StepContext stepCtx;
        if (step.getExec() instanceof CommandExecDescriptor)
            stepCtx = getSimpleStepContext(toolsRepos, parameters, workingDirectory, step);
        else
            stepCtx = getComposeStepContext(pipelinesRepos, workingDirectory, step);
        return stepCtx;
    }

    private static void setInputs(IPipelineDescriptor pipelineDescriptor,
                                  PipelineContext pipelineContext) throws EngineException {
        Map<String, Collection<String>> inputs = getInputs(pipelineContext, pipelineDescriptor.getSteps());
        pipelineContext.getPipelineEnvironment().setInputs(inputs);
    }

    private static StepContext getComposeStepContext(Map<String, IPipelinesRepository> pipelinesRepos,
                                                     String workingDirectory, IStepDescriptor step) throws EngineException {
        String stepId = step.getId();
        IPipelineExecDescriptor exec = (IPipelineExecDescriptor) step.getExec();
        Environment environment = getStepEnvironment(stepId, workingDirectory);
        IPipelinesRepository repo = pipelinesRepos.get(exec.getRepositoryId());
        IPipelineDescriptor descriptor = DescriptorsUtils.getPipelineDescriptor(exec, stepId, repo);
        return new ComposeStepContext(stepId, environment, step, descriptor);
    }

    private static StepContext getSimpleStepContext(Map<String, IToolsRepository> toolsRepos,
                                                    Map<String, Object> parameters,
                                                    String workingDirectory, IStepDescriptor step) throws EngineException {

        String stepId = step.getId();
        CommandExecDescriptor exec = (CommandExecDescriptor) step.getExec();
        IToolsRepository repo = toolsRepos.get(exec.getRepositoryId());
        String toolName = ((CommandExecDescriptor) step.getExec()).getToolName();
        ICommandDescriptor commandDescriptor = DescriptorsUtils.getCommand(repo, exec);
        Environment environment = getStepEnvironment(stepId, workingDirectory);
        IToolDescriptor tool = DescriptorsUtils.getTool(repo, toolName, stepId);
        IExecutionContextDescriptor execCtx = DescriptorsUtils.getExecutionContext(tool.getExecutionContexts(), step.getExecutionContext(), parameters);
        assert execCtx != null;
        ICommandBuilder commandBuilder = getCommandBuilder(execCtx.getContext());
        return new SimpleStepContext(stepId, environment, step, commandDescriptor, commandBuilder, execCtx);
    }

    private static IPipelineDescriptor getPipelineDescriptor(String stepId, PipelineContext pipeline,
                                                             IStepDescriptor step) throws EngineException {
        IRepositoryDescriptor repoDesc = RepositoryUtils.getRepositoryById(pipeline.getDescriptor().getRepositories(),
                step.getExec().getRepositoryId());
        if (!(repoDesc instanceof IPipelineRepositoryDescriptor))
            throw new EngineException("Step " + stepId + " doesn't refers a pipeline repository.");
        IPipelineRepositoryDescriptor repoDesc1 = (IPipelineRepositoryDescriptor) repoDesc;
        IPipelinesRepository repo = RepositoryUtils.getPipelinesRepository(repoDesc1, pipeline.getParameters());
        IPipelineExecDescriptor pipelineExec = (IPipelineExecDescriptor) step.getExec();
        return DescriptorsUtils.getPipelineDescriptor(pipelineExec, stepId, repo);
    }

    private static ICommandBuilder getCommandBuilder(String contextName) {
        return CommandBuilderSupplier.getCommandBuilder(contextName);
    }

    private static Environment getStepEnvironment(String id, String workingDirectory) {
        String stepWorkDir = workingDirectory + File.separatorChar + id;
        Environment environment = new Environment();
        environment.setOutputsDirectory(stepWorkDir + File.separatorChar  + "outputs");
        environment.setWorkDirectory(stepWorkDir);
        return environment;
    }

    private static PipelineEnvironment getPipelineEnvironment(Arguments arguments, String workingDirectory) {
        PipelineEnvironment environment = new PipelineEnvironment();
        environment.setCpu(arguments.cpus);
        environment.setDisk(arguments.disk);
        environment.setMemory(arguments.mem);
        environment.setOutputsDirectory(arguments.outPath);
        environment.setWorkDirectory(workingDirectory);
        return environment;
    }

    private static Map<String, Collection<String>> getInputs(PipelineContext pipeline,
                                                 Collection<IStepDescriptor> steps) throws EngineException {
        Map<String, Collection<String>> inputs = new HashMap<>();

        for (IStepDescriptor step : steps) {
            String id = step.getId();
            for (String input : getInputs(pipeline, pipeline.getStepsContexts().get(id))) {
                if (input.contains(File.separatorChar + "") && !input.contains("$")) {
                    if (!inputs.containsKey(id))
                        inputs.put(id, new LinkedList<>());
                    inputs.get(id).add(input);
                }
            }
        }
        return inputs;
    }

    private static Arguments getStepArguments(PipelineContext pipeline, StepContext stepCtx) throws EngineException {
        Environment environment = pipeline.getPipelineEnvironment();
        Arguments args = new Arguments();
        int mem = getValue(pipeline, stepCtx.getId(), environment.getMemory(), ICommandDescriptor::getRecommendedMemory);
        int cpus = getValue(pipeline, stepCtx.getId(), environment.getCpu(), ICommandDescriptor::getRecommendedCpu);
        int disk = getValue(pipeline, stepCtx.getId(), environment.getDisk(), ICommandDescriptor::getRecommendedDisk);
        int pipelineMem = pipeline.getPipelineEnvironment().getMemory();
        int pipelineCpu = pipeline.getPipelineEnvironment().getCpu();
        int pipelineDisk = pipeline.getPipelineEnvironment().getDisk();
        args.mem = mem > pipelineMem ? pipelineMem : mem;
        args.cpus = cpus > pipelineCpu ? pipelineCpu : cpus;
        args.disk = disk > pipelineDisk ? pipelineDisk : disk;
        return args;
    }

    private static int getValue(PipelineContext pipeline, String stepID, int value,
                                Function<ICommandDescriptor, Integer> func) throws EngineException {

        StepContext stepCtx = pipeline.getStepsContexts().get(stepID);
        if (stepCtx instanceof SimpleStepContext) {
            Integer stepValue = func.apply(((SimpleStepContext) stepCtx).getCommandDescriptor());
            return value < stepValue ? value : stepValue;
        } else if (stepCtx instanceof  ComposeStepContext){
            int highestValueFromPipeline = getHighestValueFromPipeline(pipeline, (ComposeStepContext) stepCtx, func);
            return value < highestValueFromPipeline ? value : highestValueFromPipeline;
        }
        return value;
    }

    // NÃO ESTOU A CONSIDERAR OS STEPS K SEJAM SUBPIPELINE DESTE SUBPIPELINE
    private static int getHighestValueFromPipeline(PipelineContext pipeline, ComposeStepContext stepCtx,
                                                   Function<ICommandDescriptor, Integer> func)
                                                    throws EngineException {
        IPipelineDescriptor pipelineDesc = stepCtx.getPipelineDescriptor();
        Map<String, Object> parameters = pipeline.getParameters();
        Map<String, IToolsRepository> toolsRepos = getToolsRepositories(pipelineDesc.getRepositories(), parameters);

        int highest = 0;
        for (IStepDescriptor stepDesc : pipelineDesc.getSteps()) {
            IExecDescriptor execDesc = stepDesc.getExec();
            if (execDesc instanceof ICommandDescriptor) {
                IToolsRepository toolsRepo = toolsRepos.get(execDesc.getRepositoryId());
                ICommandDescriptor commandDescriptor = DescriptorsUtils.getCommand(toolsRepo, (ICommandExecDescriptor) execDesc);
                int value = func.apply(commandDescriptor);
                if (highest < value) {
                    highest = value;
                }
            }
        }
        return highest;
    }

    private static Collection<String> getInputs(PipelineContext pipeline, StepContext stepCtx) throws EngineException {
        Collection<String> inputs = new LinkedList<>();

        for (IInputDescriptor inputDescriptor : stepCtx.getStep().getInputs()) {
            inputs.add(getInputValue(inputDescriptor, pipeline));
        }
        return inputs;
    }

    // NÃO ESTOU A TER EM CONTA QUE OS INPUTS CHAIN POSSAM SER OUTPUTS DEPENDENTES DE INPUTS
    private static String getChainInputValue(IChainInputDescriptor chainInput, PipelineContext pipeline) throws EngineException {
        StepContext stepCtx = pipeline.getStepsContexts().get(chainInput.getStepId());
        Object value = getOutputValue(chainInput.getOutputName(), stepCtx, pipeline);
        return value.toString();
    }

    private static String getSimpleInputValue(ISimpleInputDescriptor inputDescriptor) {
        Object inputValue = inputDescriptor.getValue();
        if (inputValue == null)
            inputValue = "";
        return inputValue.toString();
    }

    private static String getParameterInputValue(IInputDescriptor inputDescriptor, Map<String, Object> parameters) throws EngineException {
        Object inputValue = DescriptorsUtils.getInputValue(inputDescriptor, parameters);
        if (inputValue == null)
            inputValue = "";
        return inputValue.toString();
    }

    private static Object getDependentOutputValue(StepContext stepCtx, IOutputDescriptor outputDescriptor,
                                                  PipelineContext pipeline) throws EngineException {
        String outputValue = outputDescriptor.getValue();
        StringBuilder value = new StringBuilder();

        if (outputValue.indexOf("$") != outputValue.lastIndexOf("$")) {
            String[] splittedByDependency = outputValue.split("$");
            for (String str : splittedByDependency) {
                value.append(getDependentOutputValue(stepCtx, pipeline, str));
            }
        } else {
            String val = outputValue.substring(outputValue.indexOf("$") + 1);
            String inputValue = getDependentOutputValue(stepCtx, pipeline, val);
            value.append(inputValue);
        }

        if (value.length() == 0)
            throw new EngineException("Error loading output " + outputDescriptor.getName() + " value");
        return value.toString();
    }

    private static String getDependentOutputValue(StepContext stepCtx, PipelineContext pipeline, String output) throws EngineException {

        Collection <IInputDescriptor> orderedInputs = orderInputsByNameLength(stepCtx.getStep().getInputs());

        for (IInputDescriptor input : orderedInputs)
            if (output.contains(input.getInputName())) {
                String inputValue = getInputValue(input, pipeline);
                return output.replace(input.getInputName(), inputValue);
            }

        return "";
    }

    private static Collection<IInputDescriptor> orderInputsByNameLength(Collection<IInputDescriptor> inputs) {
        LinkedList<IInputDescriptor> orderedInputs = new LinkedList<>(inputs);

        Comparator<IInputDescriptor> inputDescriptorLengthComparator = (i0, i1) -> Integer.compare(i1.getInputName().length(), i0.getInputName().length());

        Collections.sort(orderedInputs, inputDescriptorLengthComparator);
        return orderedInputs;
    }

    private static AbstractMap.SimpleEntry<String, Object> getOutputValue(String outputName, StepContext stepCtx, PipelineContext pipeline) throws EngineException {
        if(stepCtx instanceof SimpleStepContext) {
            SimpleStepContext simpleStepCtx = (SimpleStepContext) stepCtx;
            IOutputDescriptor outputFromCommand = DescriptorsUtils.getOutputFromCommand(simpleStepCtx.getCommandDescriptor(), outputName);
            return new AbstractMap.SimpleEntry<>(outputFromCommand.getType(), outputFromCommand.getValue());
        }
        else {
            IOutputDescriptor outputFromPipeline = getOutputValueFromPipeline(outputName, pipeline, stepCtx);
            return new AbstractMap.SimpleEntry<>(outputFromPipeline.getType(), outputFromPipeline.getValue());
        }
    }

    private static IOutputDescriptor getOutputValueFromPipeline(String outputName, PipelineContext pipeline,
                                                     StepContext stepCtx) throws EngineException {
        PipelineContext currPipeline = createSubPipeline(stepCtx.getId(), pipeline);

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : currPipeline.getDescriptor().getOutputs()) {
            if (output.getOutputName().equals(outputName)) {
                StepContext insideStepCtx = currPipeline.getStepsContexts().get(output.getStepId());

                if(insideStepCtx instanceof SimpleStepContext) {
                    SimpleStepContext simpleStepCtx = (SimpleStepContext) stepCtx;
                    return DescriptorsUtils.getOutputFromCommand(simpleStepCtx.getCommandDescriptor(), outputName);
                } else {
                    PipelineContext nextPipeline = createSubPipeline(insideStepCtx.getId(), pipeline);
                    return getOutputValueFromPipeline(outputName, nextPipeline, insideStepCtx);
                }
            }
        }

        throw new EngineException("Not output " + outputName + " found for step " + stepCtx.getId() + ".");
    }
}
