package pt.isel.ngspipes.engine_core.entities;

import pt.isel.ngspipes.engine_core.entities.contexts.StepContext;

import java.util.LinkedList;
import java.util.List;

public class ExecutionNode {

    StepContext stepContext;
    List<ExecutionNode> childs;

    public StepContext getStepContext() { return stepContext; }
    public List<ExecutionNode> getChilds() { return childs; }

    public ExecutionNode(StepContext stepContext, List<ExecutionNode> childs) {
        this.stepContext = stepContext;
        this.childs = childs;
    }

    public ExecutionNode(StepContext stepContext) {
        this(stepContext, new LinkedList<>());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ExecutionNode)
            return stepContext.getId().equals(((ExecutionNode) obj).getStepContext().getId());
        return super.equals(obj);
    }
}
