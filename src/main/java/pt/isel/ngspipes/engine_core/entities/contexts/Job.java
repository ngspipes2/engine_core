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
    boolean inconclusive;

    @JsonIgnore
    private final List<Job> chainsFrom = new LinkedList<>();

    @JsonIgnore
    private final List<Job> chainsTo = new LinkedList<>();

    @JsonIgnore
    private Environment environment;

    @JsonIgnore
    private ExecutionState state;

    @JsonIgnore
    private Map<String, Output> outputsById;

    @JsonIgnore
    private Map<String, Input> inputsById;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Spread spread;

    public Job(String id, Environment environment) {
        this.id = id;
        this.environment = environment;
        this.state = new ExecutionState();
        this.parents = new LinkedList<>();
        this.inconclusive = false;
    }

    public Job() {}

    public void setId(String id) { this.id = id; }
    public String getId() { return id; }

    public void setEnvironment(Environment environment) { this.environment = environment; }
    public Environment getEnvironment() { return environment; }

    public List<Job> getChainsFrom() { return chainsFrom; }
    public void addChainsFrom(Job chainFrom) { this.chainsFrom.add(chainFrom); }

    public List<Job> getChainsTo() { return chainsTo; }
    public void addChainsTo(Job chainTo) { this.chainsTo.add(chainTo); }

    public ExecutionState getState() { return state; }
    public void setState(ExecutionState state) { this.state = state; }

    public Spread getSpread() { return spread; }
    public void setSpread(Spread spread) { this.spread = spread; }

    public Output getOutputById(String id) { return outputsById.get(id); }
    public List<Output> getOutputs() { return outputs; }
    public void setOutputs(List<Output> outputs) {
        this.outputs = outputs;
        this.outputsById = new HashMap<>();
        for (Output out : outputs)
            outputsById.put(out.getName(), out);
    }

    public Input getInputById(String id) { return inputsById.get(id); }
    public List<Input> getInputs() { return inputs; }
    public void setInputs(List<Input> inputs)  {
        this.inputs = inputs;
        this.inputsById = new HashMap<>();
        for (Input in : inputs)
            inputsById.put(in.getName(), in);
    }

    public Collection<String> getParents() { return parents;}
    public void addParent(Job parent) {
        if (!parents.contains(parent))
            parents.add(parent.id);
    }
    public void setParents(Collection<String> parents) { this.parents = parents; }

    public boolean isInconclusive() { return inconclusive; }
    public void setInconclusive(boolean inconclusive) { this.inconclusive = inconclusive; }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Job) {
            Job toCompare = (Job)obj;
           return this.id.equals(toCompare.id);
        }
        return false;
    }
}
