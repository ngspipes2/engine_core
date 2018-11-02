package pt.isel.ngspipes.engine_core.entities.contexts.strategy;

public abstract class CombineStrategy extends Strategy implements ICombineStrategy {

    private IStrategy firstStrategy;
    public IStrategy getFirstStrategy() { return this.firstStrategy; }
    public void setFirstStrategy(IStrategy firstStrategy) { this.firstStrategy = firstStrategy; }

    private IStrategy secondStrategy;
    public IStrategy getSecondStrategy() { return this.secondStrategy; }
    public void setSecondStrategy(IStrategy secondStrategy) { this.secondStrategy = secondStrategy; }



    public CombineStrategy(IStrategy firstStrategy, IStrategy secondStrategy) {
        this.firstStrategy = firstStrategy;
        this.secondStrategy = secondStrategy;
    }

    public CombineStrategy() { }

}
