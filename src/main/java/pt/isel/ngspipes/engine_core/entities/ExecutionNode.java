package pt.isel.ngspipes.engine_core.entities;

import java.util.LinkedList;
import java.util.List;

public class ExecutionNode {

    String id;
    List<ExecutionNode> childs;

    public String getId() { return id; }
    public List<ExecutionNode> getChilds() { return childs; }

    public ExecutionNode(String id, List<ExecutionNode> childs) {
        this.id = id;
        this.childs = childs;
    }

    public ExecutionNode(String id) {
        this.id = id;
        this.childs = new LinkedList<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutionNode)
            return id.equals(((ExecutionNode) obj).getId());
        return super.equals(obj);
    }
}
