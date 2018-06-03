package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IPipelineExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IParameterInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ISimpleInputDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.pipeline_repository.PipelinesRepositoryException;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IOutputDescriptor;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.util.*;

public class PipelineFactory {

    public static Pipeline create(IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters) throws EngineException {
        Map<String, IToolsRepository> toolsRepos = getToolsRepositories(pipelineDescriptor.getRepositories(), parameters);
        Map<String, IPipelinesRepository> pipelinesRepos = getPipelinesRepositories(pipelineDescriptor.getRepositories(), parameters);
        Map<String, IStepDescriptor> steps = getSteps(pipelineDescriptor.getSteps());

        return new Pipeline(toolsRepos, pipelinesRepos, steps, parameters, pipelineDescriptor);
    }

    public static Collection<JobUnit> createJobs(Pipeline pipeline, Arguments arguments) throws EngineException {
        Collection<JobUnit> jobs = new LinkedList<>();

        for (Map.Entry<String, IStepDescriptor> entry : pipeline.getSteps().entrySet()) {
            IStepDescriptor step = entry.getValue();
            JobUnit job = createJob(pipeline, step, arguments);
            jobs.add(job);
        }
        return jobs;
    }

    public static Map<String, IPipelinesRepository> getPipelinesRepositories(Collection<IRepositoryDescriptor> repositories, Map<String, Object> parameters) throws EngineException {
        Map<String, IPipelinesRepository> pipelinesRepositoryMap = new HashMap<>();

        for (IRepositoryDescriptor repo : repositories) {
            if(pipelinesRepositoryMap.containsKey(repo.getId()))
                throw new EngineException("Repositories ids can be duplicated. Id: " + repo.getId() + " is duplicated.");
            if (repo instanceof IPipelineRepositoryDescriptor) {
                IPipelinesRepository pipelinesRepo = RepositoryUtils.getPipelinesRepository((IPipelineRepositoryDescriptor) repo, parameters);
                pipelinesRepositoryMap.put(repo.getId(), pipelinesRepo);
            }
        }

        return pipelinesRepositoryMap;
    }

    public static Map<String,IToolsRepository> getToolsRepositories(Collection<IRepositoryDescriptor> repositories, Map<String,Object> parameters) throws EngineException {
        Map<String, IToolsRepository> toolsRepositoryMap = new HashMap<>();

        for (IRepositoryDescriptor repo : repositories) {
            if(toolsRepositoryMap.containsKey(repo.getId()))
                throw new EngineException("Repositories ids can be duplicated. Id: " + repo.getId() + " is duplicated.");
            if (repo instanceof IToolRepositoryDescriptor) {
                IToolsRepository toolsRepo = RepositoryUtils.getToolsRepository((IToolRepositoryDescriptor) repo, parameters);
                toolsRepositoryMap.put(repo.getId(), toolsRepo);
            }
        }

        return toolsRepositoryMap;
    }

    public static Map<String, IStepDescriptor> getSteps(Collection<IStepDescriptor> stepsList) throws EngineException {
        Map<String, IStepDescriptor> steps = new HashMap<>();

        for (IStepDescriptor step : stepsList) {
            if(steps.containsKey(step.getId()))
                throw new EngineException("Steps ids can be duplicated. Id: " + step.getId() + " is duplicated.");
            steps.put(step.getId(), step);
        }

        return steps;
    }

    public static Pipeline create(Pipeline pipeline, Collection<JobUnit> jobs) {
        return new Pipeline(pipeline.getToolsRepositories(), pipeline.getPipelinesRepositories(), pipeline.getSteps(),
                pipeline.getParameters(), pipeline.getDescriptor(), jobs);
    }

    public static Pipeline createSubPipeline(IStepDescriptor step, Pipeline pipeline) throws EngineException {
        IPipelineExecDescriptor exec = (IPipelineExecDescriptor) step.getExec();
        IPipelinesRepository pipelinesRepository = pipeline.getPipelinesRepositories().get(exec.getRepositoryId());
        IPipelineDescriptor pipelineDescriptor = null;
        try {
            pipelineDescriptor = pipelinesRepository.get(exec.getPipelineName());
        } catch (PipelinesRepositoryException e) {
            throw new EngineException("Error loading inside step pipeline: " + step.getId() + ".", e);
        }
        return create(pipelineDescriptor, pipeline.getParameters());
    }



    private static JobUnit createJob(Pipeline pipeline, IStepDescriptor step, Arguments arguments) throws EngineException {

        String workingDirectory = pipeline + Calendar.getInstance().getTime().toString();
        ExecutionState state = new ExecutionState();
        state.setState(StateEnum.STAGING);
        Collection<String> inputs = getInputs(pipeline, step);
        Collection<String> outputs = getOutputs(pipeline, step);

        return new JobUnit(step.getId(), arguments.mem, arguments.cpus, arguments.disk, inputs, outputs,
                            arguments.outPath, workingDirectory, state);
    }

    private static Collection<String> getOutputs(Pipeline pipeline, IStepDescriptor step) throws EngineException {
        Collection<String> outputs = new LinkedList<>();

        IExecDescriptor exec = step.getExec();
        IToolsRepository toolsRepository = pipeline.getToolsRepositories().get(exec.getRepositoryId());
        ICommandDescriptor commandDescriptor = ToolsUtils.getCommand(toolsRepository, (ICommandExecDescriptor) exec);

        for (IOutputDescriptor outputDescriptor : commandDescriptor.getOutputs()) {
            outputs.add(getOutputValue(outputDescriptor, step.getInputs(), pipeline));
        }

        return outputs;
    }

    private static String getOutputValue(IOutputDescriptor outputDescriptor, Collection<IInputDescriptor> inputs,
                                         Pipeline pipeline) throws EngineException {
        if (outputDescriptor.getValue().contains("$"))
            return getDependentOutputValue(inputs, outputDescriptor, pipeline);
        else
            return outputDescriptor.getValue();
    }

    private static String getDependentOutputValue(Collection<IInputDescriptor> inputs, IOutputDescriptor outputDescriptor,
                                                  Pipeline pipeline) throws EngineException {
        String outputValue = outputDescriptor.getValue();
        String[] splittedByDependency = outputValue.split("$");
        StringBuilder value = new StringBuilder();

        for (String str : splittedByDependency) {
            if (str.contains("/")) {
                int slashIdx = str.indexOf("/");
                String inputName = str.substring(0, slashIdx);
                String inputValue = getInputValue(inputs, inputName, pipeline);
                value.append(inputValue).append(str.substring(slashIdx));
            } else
                value.append(str);
        }

        if (value.length() == 0)
            throw new EngineException("Error loading output " + outputDescriptor.getName() + " value");
        return value.toString();
    }

    private static String getInputValue(Collection<IInputDescriptor> inputs, String inputName, Pipeline pipeline)
                                        throws EngineException {
        for (IInputDescriptor input : inputs)
            if (inputName.equals(input.getInputName()))
                return getInputValueAsString(input, pipeline);
        return "";
    }

    private static Collection<String> getInputs(Pipeline pipeline, IStepDescriptor step) throws EngineException {
        Collection<String> inputs = new LinkedList<>();

        for (IInputDescriptor inputDescriptor : step.getInputs()) {
            inputs.add(getInputValueAsString(inputDescriptor, pipeline));
        }

        return inputs;
    }

    // NÃO ESTOU A TER EM CONTA QUE OS INPUTS CHAIN POSSAM SER OUTPUTS DEPENDENTES DE INPUTS
    private static String getInputValueAsString(IInputDescriptor inputDescriptor, Pipeline pipeline) throws EngineException {

        if (inputDescriptor instanceof IParameterInputDescriptor) {
            Object inputValue = ToolsUtils.getInputValue(inputDescriptor, pipeline.getParameters());
            if (inputValue == null)
                inputValue = "";
            return inputValue.toString();
        } else if (inputDescriptor instanceof ISimpleInputDescriptor) {
            Object inputValue = ((ISimpleInputDescriptor) inputDescriptor).getValue();
            if (inputValue == null)
                inputValue = "";
            return inputValue.toString();
        } else if (inputDescriptor instanceof IChainInputDescriptor) {
            IChainInputDescriptor chainInput = (IChainInputDescriptor) inputDescriptor;
            IStepDescriptor step = pipeline.getSteps().get(chainInput.getStepId());
            return getOutputValue(chainInput, step, pipeline);
        }

        return "";
    }

    private static String getOutputValue(IChainInputDescriptor chainInput, IStepDescriptor step,
                                         Pipeline pipeline) throws EngineException {
        IExecDescriptor exec = step.getExec();
        if(exec instanceof ICommandExecDescriptor)
            return getOutputValueFromTool(chainInput, pipeline, step);
        else
            return getOutputValueFromPipeline(chainInput, pipeline, step);
    }

    private static String getOutputValueFromTool(IChainInputDescriptor chainInput, Pipeline pipeline,
                                                 IStepDescriptor step) throws EngineException {
        IExecDescriptor exec = step.getExec();
        IToolsRepository toolsRepository = pipeline.getToolsRepositories().get(exec.getRepositoryId());
        ICommandDescriptor commandDescriptor = ToolsUtils.getCommand(toolsRepository, (ICommandExecDescriptor) exec);
        return ToolsUtils.getOutputFromCommand(commandDescriptor, chainInput.getOutputName()).toString();
    }

    private static String getOutputValueFromPipeline(IChainInputDescriptor chainInput, Pipeline pipeline,
                                                     IStepDescriptor step) throws EngineException {
        Pipeline currPipeline = createSubPipeline(step, pipeline);

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : currPipeline.getDescriptor().getOutputs()) {
            if (output.getOutputName().equals(chainInput.getOutputName())) {
                IStepDescriptor insideStep = currPipeline.getSteps().get(output.getStepId());

                if(insideStep.getExec() instanceof ICommandExecDescriptor) {
                    return getOutputValueFromTool(chainInput, pipeline, insideStep);
                } else {
                    Pipeline nextPipeline = createSubPipeline(insideStep, pipeline);
                    return getOutputValueFromPipeline(chainInput, nextPipeline, insideStep);
                }
            }
        }
        return "";
    }

}
