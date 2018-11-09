package pt.isel.ngspipes.engine_core.entities.contexts;

import pt.isel.ngspipes.engine_core.entities.contexts.strategy.ICombineStrategy;

import java.util.Collection;

public class Spread {

    private Collection<String> inputs;
    private ICombineStrategy strategy;

    public Spread(Collection<String> inputs, ICombineStrategy strategy) {
        this.inputs = inputs;
        this.strategy = strategy;
    }

    public Spread() {}

    public Collection<String> getInputs() { return inputs; }
    public ICombineStrategy getStrategy() { return strategy; }

}
