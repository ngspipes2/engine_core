package pt.isel.ngspipes.engine_core.entities.contexts;

import pt.isel.ngspipes.engine_core.entities.contexts.strategy.ICombineStrategy;

import java.util.Collection;

public class Spread {

    private Collection<String> inputsToSpread;
    private ICombineStrategy strategy;

    public Spread(Collection<String> inputsToSpread, ICombineStrategy strategy) {
        this.inputsToSpread = inputsToSpread;
        this.strategy = strategy;
    }

    public Collection<String> getInputsToSpread() { return inputsToSpread; }
    public ICombineStrategy getStrategy() { return strategy; }
}
