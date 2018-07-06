package pt.isel.ngspipes.engine_core.tasks;

public class BasicTask<T> extends Task<T> {

    protected final Action<T> action;



    public BasicTask(Action<T> action) {
        this.action = action;
    }



    @Override
    protected T execute() throws Exception {
        return this.action.run();
    }
}
