package pt.isel.ngspipes.engine_core.entities;

import java.util.LinkedList;
import java.util.List;

public class ExecutionNode {

    JobUnit job;
    List<ExecutionNode> childs;

    public JobUnit getJob() { return job; }
    public List<ExecutionNode> getChilds() { return childs; }

    public ExecutionNode(JobUnit job, List<ExecutionNode> childs) {
        this.job = job;
        this.childs = childs;
    }

    public ExecutionNode(JobUnit job) {
        this(job, new LinkedList<>());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutionNode)
            return job.id.equals(((ExecutionNode) obj).job.id);
        return super.equals(obj);
    }
}
