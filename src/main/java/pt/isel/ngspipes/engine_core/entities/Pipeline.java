package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.util.Map;

public class Pipeline {

    private Map<String, IToolsRepository> toolsRepositories;
    private Map<String, IPipelinesRepository> pipelinesRepositories;
    private Map<String, IStepDescriptor> steps;
    private Map<String, Object> parameters;
    private IPipelineDescriptor descriptor;
    private Map<String, JobUnit> jobUnits;

    public Pipeline(Map<String, IToolsRepository> toolsRepositories, Map<String,
                    IPipelinesRepository> pipelinesRepositories, Map<String, IStepDescriptor> steps,
                    Map<String, Object> parameters, IPipelineDescriptor descriptor) {
        this.toolsRepositories = toolsRepositories;
        this.pipelinesRepositories = pipelinesRepositories;
        this.steps = steps;
        this.parameters = parameters;
        this.descriptor = descriptor;
    }

    public Pipeline(Map<String, IToolsRepository> toolsRepositories, Map<String,
            IPipelinesRepository> pipelinesRepositories, Map<String, IStepDescriptor> steps,
                    Map<String, Object> parameters, IPipelineDescriptor descriptor, Map<String, JobUnit> jobsUnits) {
        this(toolsRepositories, pipelinesRepositories, steps, parameters, descriptor);
        this.jobUnits = jobsUnits;
    }

    public Map<String, IToolsRepository> getToolsRepositories() { return toolsRepositories; }
    public Map<String, IPipelinesRepository> getPipelinesRepositories() { return pipelinesRepositories; }
    public Map<String, IStepDescriptor> getSteps() { return steps; }
    public Map<String, Object> getParameters() { return parameters; }
    public IPipelineDescriptor getDescriptor() { return descriptor; }
    public Map<String, JobUnit> getJobUnits() { return jobUnits; }
}
