package pt.isel.ngspipes.engine_core.tasks;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskFactory implements AutoCloseable {

    private static final TaskFactory INSTANCE = new TaskFactory();



    public static <T> Task<T> createTask(Task.Action<T> action) {
        return INSTANCE.create(action);
    }

    public static <T> Task<T> createTask(Task.Action<T> action, int numberOfTries, long sleepTimeBetweenTries) {
        return INSTANCE.create(action, numberOfTries, sleepTimeBetweenTries);
    }

    public static Task<Void> createTask(Runnable action) {
        return INSTANCE.create(action);
    }

    public static Task<Void> createTask(Runnable action, int numberOfTries, long sleepTimeBetweenTries) {
        return INSTANCE.create(action, numberOfTries, sleepTimeBetweenTries);
    }

    public static <T> Task<T> createAndExecuteTask(Task.Action<T> action) {
        return INSTANCE.createAndExecute(action);
    }

    public static <T> Task<T> createAndExecuteTask(Task.Action<T> action, int numberOfTries, long sleepTimeBetweenTries) {
        return INSTANCE.createAndExecute(action, numberOfTries, sleepTimeBetweenTries);
    }

    public static Task<Void> createAndExecuteTask(Runnable action) {
        return INSTANCE.createAndExecute(action);
    }

    public static Task<Void> createAndExecuteTask(Runnable action, int numberOfTries, long sleepTimeBetweenTries) {
        return INSTANCE.createAndExecute(action, numberOfTries, sleepTimeBetweenTries);
    }

    public static <T> Task<Collection<T>> whenAllTasks(Collection<Task<T>> tasks) {
        return INSTANCE.whenAll(tasks);
    }

    public static void dispose() throws Exception {
        INSTANCE.close();
    }



    private ExecutorService pool;



    public TaskFactory(int nThreads) {
        this.pool = Executors.newFixedThreadPool(nThreads);
    }

    public TaskFactory() {
        this(Runtime.getRuntime().availableProcessors());
    }



    private <T> void execute(Task<T> task) {
        task.scheduledEvent.fire();
        pool.submit(task);
    }


    public <T> Task<T> create(Task.Action<T> action) {
        return new BasicTask<T>(action){
            @Override
            protected void execute(Task<?> task){
                TaskFactory.this.execute(task);
            }
        };
    }

    public <T> Task<T> create(Task.Action<T> action, int numberOfTries, long sleepTimeBetweenTries) {
        return new TryBasicTask<T>(action, numberOfTries, sleepTimeBetweenTries){
            @Override
            protected void execute(Task<?> task){
                TaskFactory.this.execute(task);
            }
        };
    }

    public Task<Void> create(Runnable action) {
        return new BasicTask<Void>(() -> {
            action.run();
            return null;
        }){
            @Override
            protected void execute(Task<?> task){
                TaskFactory.this.execute(task);
            }
        };
    }

    public Task<Void> create(Runnable action, int numberOfTries, long sleepTimeBetweenTries) {
        return new TryBasicTask<Void>(() -> {
            action.run();
            return null;
        }, numberOfTries, sleepTimeBetweenTries){
            @Override
            protected void execute(Task<?> task){
                TaskFactory.this.execute(task);
            }
        };
    }


    public <T> Task<T> createAndExecute(Task.Action<T> action) {
        Task<T> task = create(action);

        execute(task);

        return task;
    }

    public <T> Task<T> createAndExecute(Task.Action<T> action, int numberOfTries, long sleepTimeBetweenTries) {
        Task<T> task = create(action, numberOfTries, sleepTimeBetweenTries);

        execute(task);

        return task;
    }

    public Task<Void> createAndExecute(Runnable action) {
        Task<Void> task = create(action);

        execute(task);

        return task;
    }

    public Task<Void> createAndExecute(Runnable action, int numberOfTries, long sleepTimeBetweenTries) {
        Task<Void> task = create(action, numberOfTries, sleepTimeBetweenTries);

        execute(task);

        return task;
    }


    public <T> Task<Collection<T>> whenAll(Collection<Task<T>> tasks) {
        Collection<T> results = new LinkedList<>();
        Exception[] exception = new Exception[1];

        Task<Collection<T>> resultsTask = create(() -> {
            if(exception[0] != null)
                throw exception[0];

            return results;
        });

        if(tasks.isEmpty())
            execute(resultsTask);
        else
            checkForFinish(tasks, results, exception, resultsTask);

        return resultsTask;
    }

    private <T> void checkForFinish(Collection<Task<T>> tasks, Collection<T> results, Exception[] exception, Task<Collection<T>> resultsTask) {
        for(Task<T> task : tasks){
            task.finishedEvent.addListener(() -> {
                synchronized (results) {
                    if(!results.isEmpty() || exception[0]!=null)
                        return;

                    for(Task<T> t : tasks)
                        if(!t.finishedEvent.hasFired())
                            return;

                    for(Task<T> t : tasks) {
                        if(t.failedEvent.hasFired()){
                            exception[0] = t.getException();
                            break;
                        }

                        results.add(t.getValue());
                    }

                    execute(resultsTask);
                }
            });
        }
    }



    public void close() throws Exception {
        pool.shutdown();
    }
}
