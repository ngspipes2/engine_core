package pt.isel.ngspipes.engine_core.implementations;

import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.JobUnit;
import pt.isel.ngspipes.engine_core.entities.Pipeline;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.tasks.Task;
import pt.isel.ngspipes.engine_core.tasks.TaskFactory;
import pt.isel.ngspipes.engine_core.utils.Pair;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EngineLocalDefault extends Engine {

    public EngineLocalDefault(String workingDirectory) {
        super(workingDirectory);
    }

    @Override
    protected void run(Collection<ExecutionNode> executionGraph, Pipeline pipeline, String executionId) {

        Map<JobUnit, Pair<ExecutionNode, Task>> taskMap = getTasks(executionGraph, pipeline, executionId, new HashMap<>());

        for (Map.Entry<JobUnit, Pair<ExecutionNode, Task>> entry : taskMap.entrySet()) {
            if (entry.getValue().getVal1().getChilds().isEmpty()) {
                runWhenAll(entry.getKey(), taskMap);
            }
        }

        /*new Thread(() -> {
            try {

            } catch (EngineException e) {
                ExecutionState executionState = states.get(executionId);
                executionState.setException(e);
                executionState.setState(StateEnum.FAILED);
            }
        }).start();*/
    }

    private void runWhenAll(JobUnit job, Map<JobUnit, Pair<ExecutionNode, Task>> taskMap) {
        throw new NotImplementedException();
    }

    private Map<JobUnit, Pair<ExecutionNode, Task>> getTasks(Collection<ExecutionNode> executionGraph, Pipeline pipeline,
                                                    String executionId, Map<JobUnit, Pair<ExecutionNode, Task>> taskMap) {

        for (ExecutionNode node : executionGraph) {
            JobUnit job = node.getJob();
            Task<Void> task = TaskFactory.createTask(() -> runTask(job, executionId, pipeline));
            Pair<ExecutionNode, Task> pair = new Pair<>(node, task);
            taskMap.put(job, pair);
            return getTasks(node.getChilds(), pipeline, executionId, taskMap);
        }

        return taskMap;
    }

    private void runTask(JobUnit job, String executionId, Pipeline pipeline) {
        throw new NotImplementedException();
    }

    @Override
    public boolean stop(String executionId) throws EngineException {
        throw new NotImplementedException();
    }
}
