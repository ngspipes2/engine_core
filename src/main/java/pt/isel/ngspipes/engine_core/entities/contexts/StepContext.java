package pt.isel.ngspipes.engine_core.entities.contexts;

import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public abstract class StepContext {

    private final String id;
    private final Environment environment;
    private final IStepDescriptor step;
    private final ExecutionState state;
    private final Collection<StepContext> parents;
    private Map<String, Object> outputs;

    public StepContext(String id, Environment environment, IStepDescriptor step) {
        this.id = id;
        this.environment = environment;
        this.step = step;
        this.state = new ExecutionState();
        this.parents = new LinkedList<>();
    }

    public String getId() { return id; }
    public Environment getEnvironment() { return environment; }
    public IStepDescriptor getStep() { return step; }
    public ExecutionState getState() { return state; }
    public Collection<StepContext> getParents() { return parents;}

    public void setOutputs(Map<String, Object> outputs) { this.outputs = outputs; }
    public Map<String, Object> getOutputs() { return outputs; }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StepContext) {
            StepContext toCompare = (StepContext)obj;
           return this.id.equals(toCompare.id);
        }
        return false;
    }
}
