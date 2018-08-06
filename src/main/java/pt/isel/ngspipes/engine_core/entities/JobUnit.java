package pt.isel.ngspipes.engine_core.entities;

import java.util.Collection;

public class JobUnit {

    String id;
    Collection<JobUnit> parents;
    Environment environment;
    Collection<String> inputs;
    Collection<String> outputs;
    ExecutionState state;

    public JobUnit(String id, Environment environment, Collection<String> inputs,
                   Collection<String> outputs, ExecutionState state) {
        this.id = id;
        this.environment = environment;
        this.inputs = inputs;
        this.outputs = outputs;
        this.state = state;
    }

    public String getId() { return id; }

    public Collection<JobUnit> getParents() { return parents; }
    public void setParents(Collection<JobUnit> parents) { this.parents = parents; }

    public Collection<String> getInputs() { return inputs; }
    public void setInputs(Collection<String> inputs) { this.inputs = inputs; }

    public Collection<String> getOutputs() { return outputs; }
    public void setOutputs(Collection<String> outputs) { this.outputs = outputs; }

    public ExecutionState getState() { return state; }
    public void setState(ExecutionState state) { this.state = state; }

    public Environment getEnvironment() { return environment; }
}
