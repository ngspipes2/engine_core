package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.engine_common.entities.ExecutionState;

import java.util.List;

public class Status {

    private final ExecutionState pipelineStatus;
    private final List<ExecutionState> jobsStatus;

    public Status(ExecutionState pipelineStatus, List<ExecutionState> jobsStatus) {
        this.pipelineStatus = pipelineStatus;
        this.jobsStatus = jobsStatus;
    }
}
