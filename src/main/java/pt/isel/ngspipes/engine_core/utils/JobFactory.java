package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.PipelineEnvironment;
import pt.isel.ngspipes.engine_core.entities.contexts.*;
import pt.isel.ngspipes.engine_core.entities.contexts.strategy.*;
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
import pt.isel.ngspipes.pipeline_descriptor.step.spread.ISpreadDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.ICombineStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IInputStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IStrategyDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.tool_descriptor.interfaces.*;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JobFactory {

    public static Pipeline create(String executionId, IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                                  Arguments arguments, String workingDirectory) throws EngineException {

        List<Job> jobs = getJobs(pipelineDescriptor, parameters, workingDirectory);
        PipelineEnvironment environment = getPipelineEnvironment(arguments, workingDirectory);
        Pipeline pipeline = new Pipeline(jobs, executionId, environment);
        setStepsResources(pipeline, pipelineDescriptor, parameters);
        setOutputs(pipeline::setOutputs, pipelineDescriptor, pipeline.getJobs());
        return pipeline;
    }

    private static void setOutputs(Consumer<List<Output>> consumer, IPipelineDescriptor pipelineDescriptor, List<Job> jobs) {
        List<Output> outputs = new LinkedList<>();

        if (pipelineDescriptor.getOutputs().isEmpty()) {
            outputs.addAll(getJobsOutputs(jobs));
        }

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDesc : pipelineDescriptor.getOutputs()) {
            String stepId = outputDesc.getStepId();
            String outName = outputDesc.getOutputName();
            Job jobById = getJobById(jobs, stepId);
            Output outputById = Objects.requireNonNull(jobById).getOutputById(outName);
            Output output = new Output(outputDesc.getName(), jobById, outputById.getType(), outputById.getValue());
            outputs.add(output);
        }

        consumer.accept(outputs);
    }

    private static Job getJobById(List<Job> jobs, String stepId) {
        for (Job job : jobs)
            if (job.getId().equals(stepId))
                return job;
        return null;
    }

    private static Collection<Output> getJobsOutputs(List<Job> jobs) {
        List<Output> outputs = new LinkedList<>();

        for (Job job : jobs)
            outputs.addAll(job.getOutputs());

        return outputs;
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

    private static Map<String,IToolsRepository> getToolsRepositories(Collection<IRepositoryDescriptor> repositories,
                                                                     Map<String, Object> parameters) throws EngineException {
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

    private static List<Job> getJobs(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters,
                                     String workingDirectory) throws EngineException {

        Map<String, IToolsRepository> toolsRepos = getToolsRepositories(pipelineDesc.getRepositories(), parameters);
        Map<String, IPipelinesRepository> pipelinesRepos = getPipelinesRepositories(pipelineDesc.getRepositories(), parameters);
        Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap = new HashMap<>();
        addJobs(pipelineDesc, parameters, workingDirectory, toolsRepos, pipelinesRepos, "", jobsCmdMap);
        setJobsInOut(pipelineDesc, parameters, toolsRepos, pipelinesRepos, jobsCmdMap);
        return jobsCmdMap.values().stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    private static void addJobs(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters,
                                String workingDirectory, Map<String, IToolsRepository> toolsRepos,
                                Map<String, IPipelinesRepository> pipesRepos, String subId,
                                Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineException {
        for (IStepDescriptor step : pipelineDesc.getSteps()) {
            if (step.getExec() instanceof ICommandExecDescriptor) {
                addSimpleJob(parameters, workingDirectory, subId, toolsRepos, step, jobCmdMap);
            } else {
                addComposeJob(parameters, workingDirectory, step, pipesRepos, subId, jobCmdMap, pipelineDesc);
            }
        }
    }

    private static void addSimpleJob(Map<String, Object> params, String workingDirectory, String subId,
                                     Map<String, IToolsRepository> toolsRepos, IStepDescriptor step,
                                     Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap) throws EngineException {
        String stepId = step.getId();
        CommandExecDescriptor exec = (CommandExecDescriptor) step.getExec();
        IToolsRepository repo = toolsRepos.get(exec.getRepositoryId());
        IToolDescriptor tool = DescriptorsUtils.getTool(repo, exec.getToolName(), stepId);
        ICommandDescriptor cmdDesc = DescriptorsUtils.getCommandByName(tool.getCommands(), exec.getCommandName());
        ExecutionContext execCtx = getExecutionContext(params, step, tool);

        assert cmdDesc != null;
        String command = cmdDesc.getCommand();
        String jobId = stepId;
        if (!subId.isEmpty())
            jobId = generateSubJobId(subId, stepId);
        Environment environment = getStepEnvironment(jobId, workingDirectory);
        Job job = new SimpleJob(jobId, environment, command, execCtx);

        ISpreadDescriptor spread = step.getSpread();
        if (spread != null) {
            Collection<String> inputsToSpread = spread.getInputsToSpread();
            ICombineStrategy strategy = getStrategy(spread);
            Spread spreadContext = new Spread(inputsToSpread, strategy);
            job.setSpread(spreadContext);
        }

        jobsCmdMap.put(jobId, new AbstractMap.SimpleEntry<>(job, cmdDesc));
    }

    private static void addComposeJob(Map<String, Object> params, String workingDirectory, IStepDescriptor step,
                                      Map<String, IPipelinesRepository> pRepos, String subId,
                                      Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap,
                                      IPipelineDescriptor pipelineDesc) throws EngineException {
        String stepId = step.getId();
        IPipelineDescriptor pipesDescriptor = DescriptorsUtils.getPipelineDescriptor(pRepos, step);
        Collection<IRepositoryDescriptor> repositories = pipesDescriptor.getRepositories();
        Map<String, IToolsRepository> subToolsRepos = getToolsRepositories(repositories, params);
        Map<String, IPipelinesRepository> subPipesRepos = getPipelinesRepositories(repositories, params);
        updateSubPipelineInputs(pipesDescriptor, step);
        addJobs(pipesDescriptor, params, workingDirectory, subToolsRepos, subPipesRepos, subId + stepId, jobsCmdMap);
        updateChainInputsToSubPipeline(pipelineDesc, step, jobsCmdMap, pipesDescriptor);
    }

    private static void updateChainInputsToSubPipeline(IPipelineDescriptor pipeDesc, IStepDescriptor step,
                                                       Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap, IPipelineDescriptor subPipeDesc) {
        for (IStepDescriptor stepDesc : pipeDesc.getSteps()) {
            if (!stepDesc.getId().equals(step.getId())) {
                for (IInputDescriptor input : stepDesc.getInputs()) {
                    if (input instanceof IChainInputDescriptor) {
                        IChainInputDescriptor in = (IChainInputDescriptor) input;
                        if (in.getStepId().equals(step.getId())) {
                            pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor out = DescriptorsUtils.getOutputFromPipeline(subPipeDesc, in.getOutputName());
                            assert out != null;
                            Job job = getOriginJobByStepId(jobsCmdMap, out.getStepId() + "_" + step.getId());
                            assert job != null;
                            in.setStepId(job.getId());
                            in.setOutputName(out.getOutputName());
                        }
                    }
                }
            }
        }
    }

    private static void setJobsInOut(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters,
                                     Map<String, IToolsRepository> toolsRepos,
                                     Map<String, IPipelinesRepository> pipelinesRepos,
                                     Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineException {
        for (Map.Entry<String, Map.Entry<Job, ICommandDescriptor>> entry : jobCmdMap.entrySet()) {
            Job job = entry.getValue().getKey();
            String id = job.getId();
            IPipelineDescriptor pipelineDescriptor = pipelineDesc;
            IStepDescriptor step = null;
            if (job.getId().contains("_")) {
                String stepId = job.getId();
                id = id.substring(id.lastIndexOf("_") + 1);
                stepId = stepId.substring(0, stepId.indexOf("_"));
                pipelineDescriptor = getPipelineDescriptor(pipelinesRepos, pipelineDescriptor, id, job.getId());
                assert pipelineDescriptor != null;
                step = DescriptorsUtils.getStepById(pipelineDescriptor, id);
                assert step != null;
                pipelineDescriptor = DescriptorsUtils.getPipelineDescriptor(pipelinesRepos, step);
                step = DescriptorsUtils.getStepById(pipelineDescriptor, stepId);
            } else
                step = DescriptorsUtils.getStepById(pipelineDesc, id);
            List<Input> inputs = getInputs(parameters, job, jobCmdMap, toolsRepos, pipelinesRepos, pipelineDesc, step);
            job.setInputs(inputs);
            List<Output> outputs = getOutputs(job, jobCmdMap, inputs);
            job.setOutputs(outputs);
        }
    }

    private static void updateSubPipelineInputs(IPipelineDescriptor pipelineDesc, IStepDescriptor step) {

        List<IInputDescriptor> toRemove = new LinkedList<>();
        List<IInputDescriptor> toAdd = new LinkedList<>();
        List<String> visit = new LinkedList<>();

        for (IStepDescriptor subStep : pipelineDesc.getSteps()) {
            for (IInputDescriptor input : subStep.getInputs()) {
                String inputName = input.getInputName();
                if (input instanceof IParameterInputDescriptor && !visit.contains(inputName)) {
                    visit.add(inputName);
                    String parameterName = ((IParameterInputDescriptor) input).getParameterName();
                    IInputDescriptor in = DescriptorsUtils.getInputByName(step.getInputs(), parameterName);
                    assert in != null;
                    in.setInputName(inputName);
                    toAdd.add(in);
                    toRemove.add(input);
                } else if (input instanceof IChainInputDescriptor) {
                    IChainInputDescriptor in = (IChainInputDescriptor) input;
                    if (DescriptorsUtils.getStepById(pipelineDesc, in.getStepId()) != null)
                        in.setStepId(in.getStepId() + "_" + step.getId());
                }
            }
            for (IInputDescriptor in : toRemove) {
                subStep.getInputs().remove(in);
            }
            for (IInputDescriptor in : toAdd) {
                subStep.getInputs().add(in);
            }
        }
    }

    private static void setStepsResources(Pipeline pipeline, IPipelineDescriptor pipelineDescriptor,
                                          Map<String, Object> parameters) throws EngineException {

        for (Job job : pipeline.getJobs()) {
            Arguments arguments = getStepArguments(pipeline, job, pipelineDescriptor, parameters);
            job.getEnvironment().setMemory(arguments.mem);
            job.getEnvironment().setDisk(arguments.disk);
            job.getEnvironment().setCpu(arguments.cpus);
        }
    }

    private static String generateSubJobId(String subId, String stepId) {
        return stepId + "_" + subId;
    }

    private static ICombineStrategy getStrategy(ISpreadDescriptor spread) {
        ICombineStrategyDescriptor strategy = spread.getStrategy();
        return getCombineStrategy(strategy);
    }

    private static ICombineStrategy getCombineStrategy(ICombineStrategyDescriptor strategy) {

        IStrategyDescriptor first = strategy.getFirstStrategy();
        IStrategyDescriptor second = strategy.getSecondStrategy();

        if (first instanceof IInputStrategyDescriptor) {
            if (second instanceof IInputStrategyDescriptor) {
                InputStrategy in = new InputStrategy(((IInputStrategyDescriptor) first).getInputName());
                InputStrategy in1 = new InputStrategy(((IInputStrategyDescriptor) second).getInputName());
                return new OneToOneStrategy(in, in1);
            } else {
                InputStrategy in = new InputStrategy(((IInputStrategyDescriptor) first).getInputName());
                IStrategy strategy1 = getCombineStrategy((ICombineStrategyDescriptor) second);
                return new OneToManyStrategy(in, strategy1);
            }
        } else if (second instanceof IInputStrategyDescriptor) {
            InputStrategy in = new InputStrategy(((IInputStrategyDescriptor) second).getInputName());
            IStrategy strategy1 = getCombineStrategy((ICombineStrategyDescriptor) first);
            return new OneToManyStrategy(strategy1, in);
        } else {
            IStrategy strategy1 = getCombineStrategy((ICombineStrategyDescriptor) first);
            IStrategy strategy2 = getCombineStrategy((ICombineStrategyDescriptor) second);
            return new OneToManyStrategy(strategy1, strategy2);
        }

    }

    private static ExecutionContext getExecutionContext(Map<String, Object> parameters, IStepDescriptor step, IToolDescriptor tool) {
        IExecutionContextDescriptor execCtx = DescriptorsUtils.getExecutionContext(tool.getExecutionContexts(), step.getExecutionContext(), parameters);
        assert execCtx != null;
        return new ExecutionContext(execCtx.getName(), execCtx.getContext(), execCtx.getConfig());
    }

    private static List<Output> getOutputs(Job job, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap,
                                           List<Input> inputs) throws EngineException {
        List<Output> outputs = new LinkedList<>();
        List<String> visitOutputs = new LinkedList<>();
        Map<String, List<String>> usedBy = new HashMap<>();

        for (IOutputDescriptor output : jobCmdMap.get(job.getId()).getValue().getOutputs()) {
            String name = output.getName();
            if (!visitOutputs.contains(name)) {
                String outputValue = output.getValue();
                String type = output.getType();
                String value = getOutputValue(inputs, outputs, output.getName(), outputValue, visitOutputs,
                                                jobCmdMap, job, usedBy, type);
                Output outContext = new Output(output.getName(), job, type, value);
                outputs.add(outContext);
                visitOutputs.add(name);
            }
        }

        for (Output out : outputs) {
            if (usedBy.containsKey(out.getName()))
                out.setUsedBy(usedBy.get(out.getName()));
        }

        return outputs;
    }

    private static String getOutputValue(List<Input> inputs, List<Output> outputs, String outputName,
                                         String outputValue, List<String> visit,
                                         Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap,
                                         Job job, Map<String, List<String>> usedBy, String type) throws EngineException {
        StringBuilder value = new StringBuilder();
        if (outputValue.contains("$")) {
            if (outputValue.indexOf("$") != outputValue.lastIndexOf("$")) {
                String[] splittedByDependency = outputValue.split("$");
                for (String val : splittedByDependency) {
                    dependentOutValue(inputs, outputs, visit, jobCmdMap, job, value, val, usedBy, outputName, type);
                }
            } else {
                int idx = outputValue.indexOf("$") + 1;
                String val = outputValue.substring(idx);
                dependentOutValue(inputs, outputs, visit, jobCmdMap, job, value, val, usedBy, outputName, type);
            }
        } else {
            value.append(outputValue);
        }
        return value.toString();
    }

    private static void dependentOutValue(List<Input> inputs, List<Output> outputs, List<String> visit,
                                          Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap, Job job,
                                          StringBuilder value, String outName, Map<String, List<String>> usedBy,
                                          String outputName, String type) throws EngineException {
        int idxLast = outName.indexOf(File.separatorChar);
        String val = outName;
        if (idxLast != -1)
            val = outName.substring(0, idxLast);

        Collection<IOutputDescriptor> cmdOutputs = jobCmdMap.get(job.getId()).getValue().getOutputs();
        List<IOutputDescriptor> orderedOutputs = getOutputsOrderedByNameLength(cmdOutputs);
        IOutputDescriptor output = null;
        for (IOutputDescriptor outputDescriptor : orderedOutputs) {
            if (val.contains(outputDescriptor.getName())) {
                output = outputDescriptor;
                break;
            }
        }

        if (output != null) {
            String outputVal = getOutputValue(inputs, outputs, output.getName(), output.getValue(), visit, jobCmdMap,
                                                job, usedBy, type);
            outputVal = outName.replace(output.getName(), outputVal);

            if (!usedBy.containsKey(output.getName()))
                usedBy.put(output.getName(), new LinkedList<>());
            if (!usedBy.get(output.getName()).contains(outputName))
                usedBy.get(output.getName()).add(outputName);
            visit.add(output.getName());
            value.append(outputVal);
        } else {
            getOutputDependentValue(inputs, value, outName);
        }
    }

    private static List<IOutputDescriptor> getOutputsOrderedByNameLength(Collection<IOutputDescriptor> outputs) {
        List<IOutputDescriptor> orderedOutputs = new LinkedList<>(outputs);

        Comparator<IOutputDescriptor> outputLengthComparator = (i0, i1) -> Integer.compare(i1.getName().length(), i0.getName().length());
        Collections.sort(orderedOutputs, outputLengthComparator);

        return orderedOutputs;
    }

    private static void getOutputDependentValue(List<Input> inputs, StringBuilder value, String outName) {
        List<Input> orderedInputs = orderInputsByNameLength(inputs);
        for (Input input : orderedInputs) {
            if (outName.contains(input.getName())) {
                String outVal = outName.replace(input.getName(), input.getValue());
                value.append(outVal);
                return;
            }
        }
    }

    private static List<Input> getInputs(Map<String, Object> params, Job job,
                                         Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap,
                                         Map<String, IToolsRepository> toolsRepos,
                                         Map<String, IPipelinesRepository> pipeRepos,
                                         IPipelineDescriptor pipelinesDesc, IStepDescriptor step)
                                                      throws EngineException {

        String jobId = job.getId();
        List<Input> inputs = new LinkedList<>();
        List<String> visitParams = new LinkedList<>();
        for (IParameterDescriptor param : jobCmdMap.get(jobId).getValue().getParameters()) {
            String name = param.getName();
            if (!visitParams.contains(name)) {
                addInput(params, step, toolsRepos, pipeRepos, pipelinesDesc, inputs, visitParams, param, name, job, jobCmdMap);
            }
        }
        return inputs;
    }

    private static void addInput(Map<String, Object> params, IStepDescriptor step, Map<String, IToolsRepository> toolsRepos,
                                 Map<String, IPipelinesRepository> pipeRepos, IPipelineDescriptor pipeDesc,
                                 List<Input> inputs, List<String> visitParams, IParameterDescriptor param,
                                 String name, Job job, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineException {
        visitParams.add(name);
        IInputDescriptor input = DescriptorsUtils.getInputByName(step.getInputs(), name);
        StringBuilder inputValue = new StringBuilder();
        List<IParameterDescriptor> subParams = (List<IParameterDescriptor>) param.getSubParameters();
        if (input == null) {
            if (subParams != null && !subParams.isEmpty()) {
                List<Input> subInputs = new LinkedList<>();
                addSubInputs(params, step, toolsRepos, pipeRepos, pipeDesc, visitParams, subParams, subInputs, jobCmdMap);
                if (!subInputs.isEmpty()) {
                    Input in = new Input(param.getName(), job, "",param.getType(), "",
                            getFix(param.getPrefix()), getFix(param.getSeparator()),
                            getFix(param.getSuffix()), subInputs);
                    inputs.add(in);
                }
            }

        } else {
            addSimpleInput(params, step, toolsRepos, pipeRepos, pipeDesc, inputs, param, name, input, inputValue, jobCmdMap);
        }
    }

    private static void addSimpleInput(Map<String, Object> params, IStepDescriptor step, Map<String, IToolsRepository> tRepos,
                                       Map<String, IPipelinesRepository> pRepos, IPipelineDescriptor pipeDesc,
                                       List<Input> inputs, IParameterDescriptor param, String name, IInputDescriptor input,
                                       StringBuilder inputValue,
                                       Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineException {
        Map.Entry<String, Map.Entry<String, String>> inVal;
        Map.Entry<String, String> inMap = null;
        String stepId, outName = "";
        inVal = getInputValue(params, tRepos, pRepos, pipeDesc, input);
        if (!inVal.getValue().getKey().isEmpty())
            inMap = inVal.getValue();
        inputValue.append(inVal.getKey());

        if (inMap != null) {
            String key = inMap.getKey();
            if (key.isEmpty()) {
                if (!(input instanceof IChainInputDescriptor)) {
                    stepId = step.getId();
                } else {
                    stepId = ((IChainInputDescriptor) input).getStepId();
                }
            } else {
                stepId = key;
                outName = inMap.getValue();
            }
        } else {
            stepId = step.getId();
        }

        Job originJob = getOriginJobByStepId(jobCmdMap, stepId);
        Input inputContext = new Input(name, originJob, outName, param.getType(),
                                        inputValue.toString(), getFix(param.getPrefix()), getFix(param.getSeparator()),
                                        getFix(param.getSuffix()), new LinkedList<>());
        inputs.add(inputContext);
    }

    private static Job getOriginJobByStepId(Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap, String stepId) {
        if (jobCmdMap.containsKey(stepId))
            return jobCmdMap.get(stepId).getKey();
        else {
            for (Map.Entry<Job, ICommandDescriptor> entry : jobCmdMap.values()) {
                Job job = entry.getKey();
                if (job.getId().contains(stepId))
                    return job;
            }
        }
        return null;
    }

    private static void addSubInputs(Map<String, Object> params, IStepDescriptor step,
                                     Map<String, IToolsRepository> toolsRepos,
                                     Map<String, IPipelinesRepository> pipeRepos,
                                     IPipelineDescriptor pipeDesc, List<String> visitParams,
                                     List<IParameterDescriptor> subParams, List<Input> subInputs, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineException {
        Map.Entry<String, Map.Entry<String, String>> inVal;
        String outName = "", stepId = "";
        for (IParameterDescriptor subParam : subParams) {
            visitParams.add(subParam.getName());
            IInputDescriptor in = DescriptorsUtils.getInputByName(step.getInputs(), subParam.getName());
            if (in != null) {
                inVal = getInputValue(params, toolsRepos, pipeRepos, pipeDesc, in);
                if (!inVal.getValue().getKey().isEmpty()) {
                    stepId = inVal.getValue().getKey();
                    outName = inVal.getValue().getValue();
                } else {
                    stepId = step.getId();
                }


                List<Input> subIns = new LinkedList<>();
                Input inputContext = new Input(in.getInputName(), jobCmdMap.get(stepId).getKey(), outName,
                        subParam.getType(), inVal.getKey(), getFix(subParam.getPrefix()),
                        getFix(subParam.getSeparator()), getFix(subParam.getSuffix()), subIns);
                subInputs.add(inputContext);
            }
        }

    }

    private static String getFix(String prefix) {
        return prefix == null ? "" : prefix;
    }

    private static Map.Entry<String, Map.Entry<String, String>> getInputValue(Map<String, Object> params,
                                                                        Map<String, IToolsRepository> toolsRepos,
                                                                        Map<String, IPipelinesRepository> pipesRepos,
                                                                        IPipelineDescriptor pipeDesc,
                                                                        IInputDescriptor input) throws EngineException {
        StringBuilder value = new StringBuilder();
        String stepId = "";
        String outName = "";

        if (input instanceof IParameterInputDescriptor) {
            value.append(getParameterInputValue(input, params));
        } else if (input instanceof ISimpleInputDescriptor) {
            value.append(getSimpleInputValue((ISimpleInputDescriptor) input));
        } else if (input instanceof IChainInputDescriptor) {
            IChainInputDescriptor input1 = (IChainInputDescriptor) input;
            outName = input1.getOutputName();
            value.append(getChainInputValue(params, toolsRepos, pipesRepos, pipeDesc, input1));
            stepId = input1.getStepId();
        }
        return new AbstractMap.SimpleEntry<>(value.toString(), new AbstractMap.SimpleEntry<>(stepId, outName));
    }

    private static String getChainInputValue(Map<String, Object> parameters, Map<String, IToolsRepository> toolsRepos,
                                             Map<String, IPipelinesRepository> pipelinesRepos,
                                             IPipelineDescriptor pipelineDesc,
                                             IChainInputDescriptor input) throws EngineException {
        Object value;
        String dependentStep = input.getStepId();
        IPipelineDescriptor pipelineDescriptor = pipelineDesc;
        if (dependentStep.contains("_")) {
            dependentStep = getStepName(dependentStep);
            pipelineDescriptor = getPipelineDescriptor(pipelinesRepos, pipelineDesc, dependentStep, input.getStepId());
        }

        IStepDescriptor stepDesc = DescriptorsUtils.getStepById(pipelineDescriptor, dependentStep);
        String outName = input.getOutputName();
        assert stepDesc != null;
        if (toolsRepos.containsKey(stepDesc.getExec().getRepositoryId())) {
            StringBuilder val = getOutputValue(parameters, toolsRepos, pipelinesRepos, pipelineDescriptor, outName, stepDesc);
            value = val.toString();
        } else {
            IPipelinesRepository pipelinesRepository = pipelinesRepos.get(stepDesc.getExec().getRepositoryId());
            IPipelineExecDescriptor pipeExec = (IPipelineExecDescriptor) stepDesc.getExec();
            pipelineDescriptor = DescriptorsUtils.getPipelineDescriptor(pipelinesRepository, pipeExec.getPipelineName());
            pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output;
            output = DescriptorsUtils.getOutputFromPipeline(pipelineDescriptor, outName);
            assert output != null;
            IStepDescriptor subStep = DescriptorsUtils.getStepById(pipelineDescriptor, output.getStepId());
            assert subStep != null;
            Map<String, IToolsRepository> subToolsRepos = getToolsRepositories(pipelineDescriptor.getRepositories(), parameters);
            Map<String, IPipelinesRepository> subPipelinesRepos = getPipelinesRepositories(pipelineDescriptor.getRepositories(), parameters);
            StringBuilder val = getOutputValue(parameters, subToolsRepos, subPipelinesRepos, pipelineDescriptor, output.getOutputName(), subStep);
            value = val.toString();
        }
        return value.toString();
    }

    private static IPipelineDescriptor getPipelineDescriptor(Map<String, IPipelinesRepository> pipelinesRepos,
                                                             IPipelineDescriptor parentPipe, String subStep, String stepId) throws EngineException {
        for (IStepDescriptor step : parentPipe.getSteps()) {
            if (step.getId().equals(subStep)) {
                return parentPipe;
            } else if (stepId.contains(step.getId())) {
                stepId = stepId.replace(stepId + "_", "");
                IPipelinesRepository pipelinesRepository = pipelinesRepos.get(step.getExec().getRepositoryId());
                IPipelineExecDescriptor pipeExec = (IPipelineExecDescriptor) step.getExec();
                IPipelineDescriptor pipelineDescriptor = DescriptorsUtils.getPipelineDescriptor(pipelinesRepository, pipeExec.getPipelineName());
                return getPipelineDescriptor(pipelinesRepos, pipelineDescriptor, subStep, stepId);
            }
        }
        return null;
    }


    private static String getStepName(String dependentStep) {
        int lastIdx = dependentStep.lastIndexOf("_");
        return dependentStep.substring(0, lastIdx);
    }

    private static StringBuilder getOutputValue(Map<String, Object> params, Map<String, IToolsRepository> toolsRepos,
                                                Map<String, IPipelinesRepository> pipelinesRepos, IPipelineDescriptor pipelineDesc,
                                                String outputName, IStepDescriptor stepDesc) throws EngineException {
        assert stepDesc != null;
        CommandExecDescriptor exec = (CommandExecDescriptor) stepDesc.getExec();
        IToolsRepository repo = toolsRepos.get(exec.getRepositoryId());
        IToolDescriptor tool = DescriptorsUtils.getTool(repo, exec.getToolName(), exec.getCommandName());
        ICommandDescriptor command = DescriptorsUtils.getCommandByName(tool.getCommands(), exec.getCommandName());
        assert command != null;
        IOutputDescriptor output = DescriptorsUtils.getOutputFromCommand(command, outputName);
        assert output != null;
        String outputValue = output.getValue();
        StringBuilder val = new StringBuilder();
        if (outputValue.contains("$")) {
            if (outputValue.indexOf("$") != outputValue.lastIndexOf("$")) {
                String[] splittedByDependency = outputValue.split("$");
                for (String str : splittedByDependency) {
                    IInputDescriptor dependentInput = DescriptorsUtils.getInputByName(stepDesc.getInputs(), str);
                    val.append(getInputValue(params, toolsRepos, pipelinesRepos, pipelineDesc, dependentInput));
                }
            } else {
                String currValue = outputValue.substring(outputValue.indexOf("$") + 1);
                int idxLast = currValue.indexOf(File.separatorChar);
                String inName = currValue;
                if (idxLast != -1)
                    inName = currValue.substring(0, idxLast);
                IInputDescriptor dependentInput = DescriptorsUtils.getInputByName(stepDesc.getInputs(), inName);
                String inVal = getInputValue(params, toolsRepos, pipelinesRepos, pipelineDesc, dependentInput).getKey();
                val.append(currValue.replace(inName, inVal));
            }
        } else {
            val.append(outputValue);
        }
        return val;
    }

    private static Environment getStepEnvironment(String id, String workingDirectory)
    {
        String stepWorkDir = workingDirectory + File.separatorChar + id;
        Environment environment = new Environment();
//        environment.setOutputsDirectory(stepWorkDir + File.separatorChar  + "outputs");
        environment.setOutputsDirectory(stepWorkDir);
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

    private static Arguments getStepArguments(Pipeline pipeline, Job stepCtx,
                                              IPipelineDescriptor pipelineDescriptor, Map<String, Object> params) throws EngineException {
        Environment environment = pipeline.getEnvironment();
        Arguments args = new Arguments();
        String stepId = stepCtx.getId();
        int mem = getValue(pipeline, stepId, environment.getMemory(), ICommandDescriptor::getRecommendedMemory, pipelineDescriptor, params);
        int cpus = getValue(pipeline, stepId, environment.getCpu(), ICommandDescriptor::getRecommendedCpu, pipelineDescriptor, params);
        int disk = getValue(pipeline, stepId, environment.getDisk(), ICommandDescriptor::getRecommendedDisk, pipelineDescriptor, params);
        int pipelineMem = pipeline.getEnvironment().getMemory();
        int pipelineCpu = pipeline.getEnvironment().getCpu();
        int pipelineDisk = pipeline.getEnvironment().getDisk();
        args.mem = mem > pipelineMem ? pipelineMem : mem;
        args.cpus = cpus > pipelineCpu ? pipelineCpu : cpus;
        args.disk = disk > pipelineDisk ? pipelineDisk : disk;
        return args;
    }

    private static int getValue(Pipeline pipeline, String stepID, int value, Function<ICommandDescriptor, Integer> func,
                                IPipelineDescriptor pipelineDesc, Map<String, Object> params) throws EngineException {

        Job stepCtx = pipeline.getJobById(stepID);
        if (stepCtx instanceof SimpleJob) {
            IStepDescriptor step = DescriptorsUtils.getStepById(pipelineDesc, stepCtx.getId());
            assert step != null;
            String repositoryId = step.getExec().getRepositoryId();
            IToolRepositoryDescriptor toolRepo = DescriptorsUtils.getToolRepositoryDescriptorById(repositoryId, pipelineDesc.getRepositories());
            assert toolRepo != null;
            IToolsRepository repo = RepositoryUtils.getToolsRepository(toolRepo, params);
            Integer stepValue = func.apply(DescriptorsUtils.getCommand(repo, (ICommandExecDescriptor) step.getExec()));
            return value < stepValue ? value : stepValue;
        } else if (stepCtx instanceof ComposeJob){
            int highestValueFromPipeline = getHighestValueFromPipeline(pipelineDesc, params, func);
            return value < highestValueFromPipeline ? value : highestValueFromPipeline;
        }
        return value;
    }

    // NÃO ESTOU A CONSIDERAR OS STEPS K SEJAM SUBPIPELINE DESTE SUBPIPELINE
    private static int getHighestValueFromPipeline(IPipelineDescriptor pipelineDesc, Map<String, Object> params,
                                                   Function<ICommandDescriptor, Integer> func) throws EngineException {
        Map<String, IToolsRepository> toolsRepos = getToolsRepositories(pipelineDesc.getRepositories(), params);

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

    private static String getSimpleInputValue(ISimpleInputDescriptor inputDescriptor) {
        Object inputValue = inputDescriptor.getValue();
        if (inputValue == null)
            inputValue = "";
        return inputValue.toString();
    }

    private static Object getParameterInputValue(IInputDescriptor inputDescriptor, Map<String, Object> parameters) {
        Object inputValue = parameters.get(((IParameterInputDescriptor) inputDescriptor).getParameterName());
        if (inputValue == null) {
            inputValue = "";
        }
        return inputValue;
    }

    private static List<Input> orderInputsByNameLength(List<Input> inputs) {
        LinkedList<Input> orderedInputs = new LinkedList<>(inputs);
        Comparator<Input> inputLengthComparator = (i0, i1) -> Integer.compare(i1.getName().length(), i0.getName().length());

        Collections.sort(orderedInputs, inputLengthComparator);
        return orderedInputs;
    }

}
