package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.engine_common.entities.Arguments;
import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.entities.PipelineEnvironment;
import pt.isel.ngspipes.engine_common.entities.contexts.*;
import pt.isel.ngspipes.engine_common.entities.factory.JobFactory;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.utils.DescriptorsUtils;
import pt.isel.ngspipes.engine_common.utils.RepositoryUtils;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IExecDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.util.*;
import java.util.function.Function;

public class PipelineFactory {

    public static Pipeline create(String executionId, IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters,
                                  Arguments arguments, String workingDirectory, String fileSeparator) throws EngineException {

        PipelineEnvironment environment = getPipelineEnvironment(arguments, workingDirectory);
        Map<String, IToolsRepository> toolsRepos = getToolsRepositories(pipelineDescriptor.getRepositories(), parameters);
        List<Job> jobs = null;
        try {
            jobs = JobFactory.getJobs(pipelineDescriptor, parameters, environment.getWorkDirectory(), fileSeparator, toolsRepos);
            Pipeline pipeline = new Pipeline(jobs, executionId, environment);
            setStepsResources(pipeline, pipelineDescriptor, toolsRepos);
            setOutputs(pipeline, pipelineDescriptor, pipeline.getJobs());
            JobFactory.expandReadyJobs(jobs, pipeline, fileSeparator);
            setChains(jobs);
            return pipeline;
        } catch (EngineCommonException e) {
            throw new EngineException("Error creating pipeline " + executionId + " intermediate representation.", e);
        }
    }

    public static PipelineEnvironment getPipelineEnvironment(Arguments arguments, String workingDirectory) {
        PipelineEnvironment environment = new PipelineEnvironment();
        environment.setCpu(arguments.cpus);
        environment.setDisk(arguments.disk);
        environment.setMemory(arguments.mem);
        environment.setOutputsDirectory(arguments.outPath == null ? workingDirectory : arguments.outPath);
        environment.setWorkDirectory(workingDirectory);
        return environment;
    }



    private static void setChains(List<Job> jobs) {
        for (Job job : jobs)
            setChains(jobs, job);
    }

    private static void setChains(List<Job> jobs, Job fromJob) {
        String jobName = fromJob.getId();
        for (Job toJob : jobs) {
            if (jobName.equals(toJob.getId()))
                continue;
            Collection<Input> chainInputs = getChainInputs(toJob);
            if (chainInputs.isEmpty())
                continue;
            if(isChainFromJob(jobName, chainInputs)) {
                if (!toJob.getChainsFrom().contains(fromJob)) {
                    toJob.addChainsFrom(fromJob);
                }
                if (!fromJob.getChainsTo().contains(toJob)) {
                    fromJob.addChainsTo(toJob);
                }
            }
        }
    }

    private static boolean isChainFromJob(String jobId, Collection<Input> chainInputs) {
        for (Input input : chainInputs) {
            if(input.getOriginStep() != null && input.getOriginStep().get(0).equals(jobId))
                return true;
        }
        return false;
    }

    private static Collection<Input> getChainInputs(Job job) {
        Collection<Input> chainInputs = new LinkedList<>();
        for (Input input : job.getInputs()) {
            List<String> originSteps = input.getOriginStep();
            if (originSteps != null && !originSteps.isEmpty() && !originSteps.get(0).equals(job.getId()))
                chainInputs.add(input);
        }
        return chainInputs;
    }

    private static void setOutputs(Pipeline pipeline, IPipelineDescriptor pipelineDescriptor, List<Job> jobs) {
        List<Output> outputs = new LinkedList<>();

        if (pipelineDescriptor.getOutputs().isEmpty()) {
            outputs.addAll(getJobsOutputs(jobs));
        }

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDesc : pipelineDescriptor.getOutputs()) {
            String stepId = outputDesc.getStepId();
            String outName = outputDesc.getOutputName();
            Job jobById = pipeline.getJobById(stepId);
            Output outputById = Objects.requireNonNull(jobById).getOutputById(outName);
            Output output = new Output(outputById.getName(), jobById, outputById.getType(), outputById.getValue());
            outputs.add(output);
        }

        pipeline.setOutputs(outputs);
    }

    private static Collection<Output> getJobsOutputs(List<Job> jobs) {
        List<Output> outputs = new LinkedList<>();

        for (Job job : jobs)
            outputs.addAll(job.getOutputs());

        return outputs;
    }

    private static Map<String,IToolsRepository> getToolsRepositories(Collection<IRepositoryDescriptor> repositories,
                                                                     Map<String, Object> parameters) throws EngineException {
        Map<String, IToolsRepository> toolsRepositoryMap = new HashMap<>();

        for (IRepositoryDescriptor repo : repositories) {
            if(toolsRepositoryMap.containsKey(repo.getId()))
                throw new EngineException("Repositories ids can be duplicated. Id: " + repo.getId() + " is duplicated.");
            if (repo instanceof IToolRepositoryDescriptor) {
                IToolRepositoryDescriptor toolsRepoDesc = (IToolRepositoryDescriptor) repo;
                IToolsRepository toolsRepo = null;
                try {
                    toolsRepo = RepositoryUtils.getToolsRepository(toolsRepoDesc, parameters);
                } catch (EngineCommonException e) {
                    throw new EngineException("Error getting " + toolsRepoDesc.getId() + " tools repository.", e);
                }
                toolsRepositoryMap.put(repo.getId(), toolsRepo);
            }
        }

        return toolsRepositoryMap;
    }


    private static void setStepsResources(Pipeline pipeline, IPipelineDescriptor pipelineDescriptor,
                                          Map<String, IToolsRepository> toolsRepos) throws EngineException {

        for (Job job : pipeline.getJobs()) {
            Arguments arguments = getStepArguments(pipeline, job, pipelineDescriptor, toolsRepos);
            job.getEnvironment().setMemory(arguments.mem);
            job.getEnvironment().setDisk(arguments.disk);
            job.getEnvironment().setCpu(arguments.cpus);
        }
    }


    private static Arguments getStepArguments(Pipeline pipeline, Job stepCtx, IPipelineDescriptor pipelineDescriptor,
                                              Map<String, IToolsRepository> toolsRepos) throws EngineException {
        Environment environment = pipeline.getEnvironment();
        Arguments args = new Arguments();
        String stepId = stepCtx.getId();
        int mem = getValue(pipeline, stepId, environment.getMemory(), ICommandDescriptor::getRecommendedMemory, pipelineDescriptor, toolsRepos);
        int cpus = getValue(pipeline, stepId, environment.getCpu(), ICommandDescriptor::getRecommendedCpu, pipelineDescriptor, toolsRepos);
        int disk = getValue(pipeline, stepId, environment.getDisk(), ICommandDescriptor::getRecommendedDisk, pipelineDescriptor, toolsRepos);
        int pipelineMem = pipeline.getEnvironment().getMemory();
        int pipelineCpu = pipeline.getEnvironment().getCpu();
        int pipelineDisk = pipeline.getEnvironment().getDisk();
        args.mem = mem > pipelineMem ? pipelineMem : mem;
        args.cpus = cpus > pipelineCpu ? pipelineCpu : cpus;
        args.disk = disk > pipelineDisk ? pipelineDisk : disk;
        return args;
    }

    private static int getValue(Pipeline pipeline, String stepID, int value, Function<ICommandDescriptor, Integer> func,
                                IPipelineDescriptor pipelineDesc, Map<String, IToolsRepository> toolsRepos) throws EngineException {

        Job stepCtx = pipeline.getJobById(stepID);
            try {
                if (stepCtx instanceof SimpleJob) {
                    IStepDescriptor step = DescriptorsUtils.getStepById(pipelineDesc, stepCtx.getId());
                    String repositoryId = step.getExec().getRepositoryId();
                    IToolsRepository repo = toolsRepos.get(repositoryId);
                    Integer stepValue = func.apply(DescriptorsUtils.getCommand(repo, (ICommandExecDescriptor) step.getExec()));
                    return value < stepValue ? value : stepValue;
                } else if (stepCtx instanceof ComposeJob){
                    int highestValueFromPipeline = getHighestValueFromPipeline(pipelineDesc.getSteps(), toolsRepos, func);
                    return value < highestValueFromPipeline ? value : highestValueFromPipeline;
                }
                return value;
            } catch (EngineCommonException e) {
                throw new EngineException("Error getting resource value.", e);
            }
    }

    // NÃO ESTOU A CONSIDERAR OS STEPS K SEJAM SUBPIPELINE DESTE SUBPIPELINE
    private static int getHighestValueFromPipeline(Collection<IStepDescriptor> steps, Map<String, IToolsRepository> toolsRepos,
                                                   Function<ICommandDescriptor, Integer> func) throws EngineException {

        int highest = 0;
        for (IStepDescriptor stepDesc : steps) {
            IExecDescriptor execDesc = stepDesc.getExec();
            if (execDesc instanceof ICommandDescriptor) {
                IToolsRepository toolsRepo = toolsRepos.get(execDesc.getRepositoryId());
                ICommandDescriptor commandDescriptor = null;
                try {
                    commandDescriptor = DescriptorsUtils.getCommand(toolsRepo, (ICommandExecDescriptor) execDesc);
                } catch (EngineCommonException e) {
                    throw new EngineException("Error getting biggest value of resource.", e);
                }
                int value = func.apply(commandDescriptor);
                if (highest < value) {
                    highest = value;
                }
            }
        }
        return highest;
    }

}
