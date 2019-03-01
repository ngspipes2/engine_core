package pt.isel.ngspipes.engine_core.entities.contexts;

import com.fasterxml.jackson.annotation.JsonIgnore;
import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.PipelineEnvironment;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Pipeline {

    private String name;
    private List<Output> outputs;
    private List<Job> jobs;

    @JsonIgnore
    private Map<String, Output> outputsById;

    @JsonIgnore
    private Map<String, Job> jobsById;

    @JsonIgnore
    private Environment environment;

    @JsonIgnore
    private Collection<ExecutionNode> graph;

    @JsonIgnore
    private ExecutionState state;

    public Pipeline(List<Job> jobs, String name, PipelineEnvironment environment) {
        this.name = name;
        this.environment = environment;
        this.jobs = jobs;
        initJobsById(jobs);
    }

    public Pipeline(List<Job> jobs, String name) {
        this(jobs, name, new PipelineEnvironment());
    }

    public Pipeline() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Environment getEnvironment() { return environment; }
    public void setEnvironment(Environment environment) { this.environment = environment; }

    public Collection<ExecutionNode> getGraph() { return graph; }
    public void setGraph(Collection<ExecutionNode> graph) { this.graph = graph; }

    public ExecutionState getState() { return state; }
    public void setState(ExecutionState state) { this.state = state; }

    public List<Output> getOutputs() { return outputs; }
    public void setOutputs(List<Output> outputs) {
        this.outputs = outputs;
        initOutputsById(outputs);
    }

    public List<Job> getJobs() { return jobs; }
    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
        initJobsById(jobs);
    }

    public Job getJobById(String id) { return jobsById.get(id); }
    public void addJobs(List<Job> jobs) {
        this.jobs.addAll(jobs);
        jobs.forEach((job) -> jobsById.put(job.getId(), job));
    }
    public void removeJobs(List<Job> jobs) {
        this.jobs.removeAll(jobs);
        jobs.forEach((job) -> jobsById.remove(job.getId()));
    }
    private void initJobsById(List<Job> jobs) {
        jobsById = new HashMap<>();
        for (Job job : jobs)
            jobsById.put(job.getId(), job);
    }

    public Output getOutputById(String id) { return outputsById.get(id); }
    private void initOutputsById(List<Output> outputs) {
        outputsById = new HashMap<>();
        for (Output output : outputs)
            outputsById.put(output.getName(), output);
    }
}
