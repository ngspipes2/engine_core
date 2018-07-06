package pt.isel.ngspipes.engine_core.tasks;

import java.util.Collection;
import java.util.LinkedList;

public class TaskEvent {

    private final Object lock = new Object();
    private final Collection<Runnable> listeners = new LinkedList<>();
    private boolean fired;


    public void addListener(Runnable listener){
        synchronized (lock) {
            listeners.add(listener);

            if(fired)
                listener.run();
        }
    }

    public void removeListener(Runnable listener){
        synchronized (lock) {
            listeners.remove(listener);
        }
    }

    public boolean await() throws InterruptedException {
        synchronized (lock) {
            while(!fired)
                lock.wait();

            return true;
        }
    }

    public boolean await(long timeout) throws InterruptedException {
        synchronized (lock) {
            long finalTime = System.currentTimeMillis() + timeout;
            long timeToFinish;

            while(true) {
                timeToFinish = finalTime - System.currentTimeMillis();

                if(fired || timeToFinish<0)
                    break;

                lock.wait(timeToFinish);
            }

            return fired;
        }
    }

    public void fire(){
        synchronized (lock) {
            Collection<Runnable> listeners = new LinkedList<>(this.listeners);

            if(!fired) {
                this.fired = true;

                lock.notifyAll();

                new Thread(() -> listeners.forEach(Runnable::run)).start();
            }
        }
    }

    public boolean hasFired(){
        synchronized (lock) {
            return this.fired;
        }
    }

}
