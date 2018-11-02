package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.engine_core.entities.contexts.Job;

import java.util.LinkedList;
import java.util.List;

public class ExecutionNode {

    Job job;
    List<ExecutionNode> childs;

    public Job getJob() { return job; }
    public List<ExecutionNode> getChilds() { return childs; }

    public ExecutionNode(Job job, List<ExecutionNode> childs) {
        this.job = job;
        this.childs = childs;
    }

    public ExecutionNode(Job job) {
        this(job, new LinkedList<>());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutionNode)
            return job.getId().equals(((ExecutionNode) obj).getJob().getId());
        return super.equals(obj);
    }
}
