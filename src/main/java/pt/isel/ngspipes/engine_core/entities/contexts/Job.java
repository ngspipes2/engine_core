package pt.isel.ngspipes.engine_core.entities.contexts;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;

import java.util.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ComposeJob.class, name = "compose"),

        @JsonSubTypes.Type(value = SimpleJob.class, name = "simple") }
)
public abstract class Job {

    private String id;
    private Collection<String> parents;
    private List<Input> inputs;
    private List<Output> outputs;

    @JsonIgnore
    private Environment environment;

    @JsonIgnore
    private ExecutionState state;

    @JsonIgnore
    private Map<String, Output> outputsById;

    @JsonIgnore
    private Map<String, Input> inputsById;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Spread spread;

    public Job(String id, Environment environment) {
        this.id = id;
        this.environment = environment;
        this.state = new ExecutionState();
        this.parents = new LinkedList<>();
    }

    public Job() {}

    public String getId() { return id; }
    public Environment getEnvironment() { return environment; }
    public ExecutionState getState() { return state; }
    public Collection<String> getParents() { return parents;}
    public Input getInputById(String id) { return inputsById.get(id); }
    public List<Input> getInputs() { return inputs; }
    public Output getOutputById(String id) { return outputsById.get(id); }
    public List<Output> getOutputs() { return outputs; }

    public Spread getSpread() { return spread; }
    public void setSpread(Spread spread) { this.spread = spread; }

    public void setOutputs(List<Output> outputs) {
        this.outputs = outputs;
        this.outputsById = new HashMap<>();
        for (Output out : outputs)
            outputsById.put(out.getName(), out);
    }

    public void setInputs(List<Input> inputs)  {
        this.inputs = inputs;
        this.inputsById = new HashMap<>();
        for (Input in : inputs)
            inputsById.put(in.getName(), in);
    }

    public void addParent(Job parent) {
        if (!parents.contains(parent))
            parents.add(parent.id);
    }

    public void setId(String id) { this.id = id; }
    public void setEnvironment(Environment environment) { this.environment = environment; }
    public void setParents(Collection<String> parents) { this.parents = parents; }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Job) {
            Job toCompare = (Job)obj;
           return this.id.equals(toCompare.id);
        }
        return false;
    }
}
