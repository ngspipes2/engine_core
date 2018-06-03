package pt.isel.ngspipes.engine_core.exception;

public class InputValidationException extends EngineException {

    public InputValidationException(String msg) {
        super(msg);
    }

    public InputValidationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
