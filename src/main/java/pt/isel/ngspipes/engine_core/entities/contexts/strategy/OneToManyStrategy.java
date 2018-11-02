package pt.isel.ngspipes.engine_core.entities.contexts.strategy;

public class OneToManyStrategy extends CombineStrategy {

    public OneToManyStrategy(IStrategy firstStrategy, IStrategy secondStrategy) {
        super(firstStrategy, secondStrategy);
    }

    public OneToManyStrategy() { }

}
