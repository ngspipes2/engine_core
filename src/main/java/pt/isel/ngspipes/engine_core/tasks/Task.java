package pt.isel.ngspipes.engine_core.tasks;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class Task<T> implements Runnable, Future<T> {

    @FunctionalInterface
    public interface Action<K> {
        K run() throws Exception;
    }

    @FunctionalInterface
    public interface LinkAction<K,P> {
        K run(Task<P> previous) throws Exception;
    }



    public final TaskEvent scheduledEvent = new TaskEvent();
    public final TaskEvent runningEvent = new TaskEvent();
    public final TaskEvent cancelledEvent = new TaskEvent();
    public final TaskEvent failedEvent = new TaskEvent();
    public final TaskEvent succeededEvent = new TaskEvent();
    public final TaskEvent finishedEvent = new TaskEvent();

    private TaskStatus status = TaskStatus.CREATED;
    public TaskStatus getStatus() { synchronized (this) { return status; } }
    private void setStatus(TaskStatus status) { synchronized (this) { this.status = status; } }

    private T value;
    public T getValue() { synchronized (this) { return value; } }
    private void setValue(T value) { synchronized (this) { this.value = value; } }

    private Exception exception;
    public Exception getException(){ synchronized (this) { return exception; } }
    private void setException(Exception exception) { synchronized (this) { this.exception = exception; } }



    public Task() {
        this.scheduledEvent.addListener(() -> this.setStatus(TaskStatus.SCHEDULED));
        this.runningEvent.addListener(() -> this.setStatus(TaskStatus.RUNNING));
        this.failedEvent.addListener(() -> this.setStatus(TaskStatus.FAILED));
        this.succeededEvent.addListener(() -> this.setStatus(TaskStatus.SUCCEEDED));
    }



    @Override
    public void run() {
        try {
            if(cancelledEvent.hasFired()) {
                this.setStatus(TaskStatus.CANCELED);
                return;
            }

            runningEvent.fire();

            if(cancelledEvent.hasFired()) {
                this.setStatus(TaskStatus.CANCELED);
                return;
            }

            this.setValue(execute());

            if(cancelledEvent.hasFired()) {
                this.setStatus(TaskStatus.CANCELED);
                return;
            }

            succeededEvent.fire();
        } catch(Exception e) {
            this.setException(e);
            failedEvent.fire();
        } finally {
            finishedEvent.fire();
        }
    }

    public T getResult() throws Exception {
        finishedEvent.await();

        if(this.getException() != null)
            throw this.getException();
        else
            return this.getValue();
    }

    public T getResult(long timeout) throws Exception {
        if(!finishedEvent.await(timeout))
            throw new TimeoutException();

        if(this.getException() != null)
            throw this.getException();
        else
            return this.getValue();
    }

    public void cancel(){
        this.cancelledEvent.fire();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        this.cancelledEvent.fire();
        return true;
    }

    @Override
    public boolean isCancelled() {
        return this.cancelledEvent.hasFired();
    }

    @Override
    public boolean isDone() {
        return this.finishedEvent.hasFired();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        try{
            return getResult();
        } catch(Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try{
            return getResult(TimeUnit.MICROSECONDS.convert(timeout, unit));
        } catch(Exception e) {
            if(e instanceof InterruptedException)
                throw (InterruptedException) e;

            if(e instanceof TimeoutException)
                throw (TimeoutException) e;

            throw new ExecutionException(e);
        }
    }


    public <K> Task<K> then(BasicTask.Action<K> action) {
        Task<K> task = new BasicTask<K>(action){
            @Override
            protected void execute(Task<?> task){
                Task.this.execute(task);
            }
        };

        this.finishedEvent.addListener(() -> execute(task));

        return task;
    }

    public <K> Task<K> then(BasicTask.LinkAction<K, T> action) {
        return then(() -> action.run(this));
    }

    public <K> Task<K> unwrap(BasicTask.LinkAction<Task<K>, T> action) {
        List<K> result = new LinkedList<>();
        List<Exception> exception = new LinkedList<>();

        Task<K> unwrapTask = new BasicTask<K>(() -> {
            if(!exception.isEmpty())
                throw exception.get(0);

            return result.get(0);
        }){
            @Override
            protected void execute(Task<?> task){
                Task.this.execute(task);
            }
        };

        Task<Task<K>> actionTask = this.then(action);

        actionTask.then((t) -> {
            if(t.getException() != null) {
                exception.add(t.getException());
                execute(unwrapTask);
            } else {
                Task<K> returnedTask = t.getValue();

                returnedTask.finishedEvent.addListener(() -> {
                    if(returnedTask.getException() != null)
                        exception.add(returnedTask.getException());
                    else
                        result.add(returnedTask.getValue());

                    execute(unwrapTask);
                });
            }

            return null;
        });

        return unwrapTask;
    }

    protected void execute(Task<?> task) {
        task.scheduledEvent.fire();
        new Thread(task).start();
    }



    protected abstract T execute() throws Exception;

}