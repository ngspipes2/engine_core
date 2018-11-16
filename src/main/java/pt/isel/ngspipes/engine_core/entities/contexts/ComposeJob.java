package pt.isel.ngspipes.engine_core.entities.contexts;

import pt.isel.ngspipes.engine_core.entities.Environment;

import java.util.List;

public class ComposeJob extends Job {

    private List<Job> jobs;

    public ComposeJob(String id, Environment environment) {
        super(id, environment);
    }

    public ComposeJob() {}

    
    public List<Job> getJobs() { return jobs; }
    public void setJobs(List<Job> jobs) { this.jobs = jobs; }
}
