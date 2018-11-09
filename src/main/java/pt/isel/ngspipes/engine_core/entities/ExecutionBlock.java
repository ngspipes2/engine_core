package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.engine_core.entities.contexts.Job;

import java.util.List;

public class ExecutionBlock {

    private final List<ExecutionNode> jobsToExecute;
    private final List<Job> jobs;
    private boolean finished;

    public ExecutionBlock(List<ExecutionNode> jobsToExecute, List<Job> jobs) {
        this.jobsToExecute = jobsToExecute;
        this.jobs = jobs;
        this.finished = false;
    }

    public List<ExecutionNode> getJobsToExecute() { return jobsToExecute; }
    public List<Job> getJobs() { return jobs; }

    public boolean isFinished() { return finished; }
    public void setFinished() { this.finished = true; }
}
