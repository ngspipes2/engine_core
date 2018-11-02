package pt.isel.ngspipes.engine_core.entities.contexts.strategy;

public interface ICombineStrategy extends IStrategy {

    IStrategy getFirstStrategy();
    void setFirstStrategy(IStrategy firstStrategy);

    IStrategy getSecondStrategy();
    void setSecondStrategy(IStrategy secondStrategy);
}
