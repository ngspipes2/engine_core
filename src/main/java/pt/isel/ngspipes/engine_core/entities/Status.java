package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.engine_common.entities.ExecutionState;

import java.util.List;
import java.util.Map;

public class Status {

    private final ExecutionState pipelineStatus;
    private final Map<String, ExecutionState> jobsStatus;

    public Status(ExecutionState pipelineStatus, Map<String, ExecutionState> jobsStatus) {
        this.pipelineStatus = pipelineStatus;
        this.jobsStatus = jobsStatus;
    }

    public ExecutionState getPipelineStatus() { return pipelineStatus; }

    public Map<String, ExecutionState> getJobsStatus() { return jobsStatus; }
}
