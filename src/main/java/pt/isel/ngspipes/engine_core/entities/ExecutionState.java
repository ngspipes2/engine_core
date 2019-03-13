package pt.isel.ngspipes.engine_core.entities;

public class ExecutionState {

    StateEnum state;
    public void setState(StateEnum state) { this.state = state; }
    public StateEnum getState() { return state; }

    Exception exception;
    public Exception getException() { return exception; }
    public void setException(Exception exception) { this.exception = exception; }

    public ExecutionState(StateEnum state, Exception exception) {
        this.state = state;
        this.exception = exception;
    }

    public ExecutionState() { }
}
