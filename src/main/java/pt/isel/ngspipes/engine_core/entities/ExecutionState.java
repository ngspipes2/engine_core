package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.engine_core.exception.EngineException;

public class ExecutionState {

    StateEnum state;
    public void setState(StateEnum state) { this.state = state; }
    public StateEnum getState() { return state; }

    EngineException exception;
    public EngineException getException() { return exception; }
    public void setException(EngineException exception) { this.exception = exception; }

    public ExecutionState(StateEnum state, EngineException exception) {
        this.state = state;
        this.exception = exception;
    }

    public ExecutionState() { }
}
