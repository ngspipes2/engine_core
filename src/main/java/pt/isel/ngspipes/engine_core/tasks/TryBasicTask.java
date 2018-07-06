package pt.isel.ngspipes.engine_core.tasks;

public class TryBasicTask<T> extends  BasicTask<T> {

    private int numberOfTries;
    private  long sleepTimeBetweenTries;



    public TryBasicTask(Action<T> action, int numberOfTries, long sleepTimeBetweenTries) {
        super(action);
        this.numberOfTries = numberOfTries;
        this.sleepTimeBetweenTries = sleepTimeBetweenTries;
    }



    @Override
    protected T execute() throws Exception {
        while(true) {
            try {
                return super.action.run();
            } catch (Exception e) {
                if(--numberOfTries <= 0)
                    throw e;
                else
                    sleep();
            }
        }
    }

    private void sleep() {
        try {
            Thread.sleep(sleepTimeBetweenTries);
        } catch (Exception ex) {

        }
    }
}
