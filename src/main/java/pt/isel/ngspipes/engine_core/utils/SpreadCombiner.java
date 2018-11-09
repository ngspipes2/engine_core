package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.contexts.strategy.ICombineStrategy;
import pt.isel.ngspipes.engine_core.entities.contexts.strategy.IStrategy;
import pt.isel.ngspipes.engine_core.entities.contexts.strategy.InputStrategy;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IInputStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IOneToManyStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IOneToOneStrategyDescriptor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SpreadCombiner {


    public static void getInputsCombination(ICombineStrategy strategy, Map<String, List<String>> inputsToSpread) throws EngineException {

        IStrategy first = strategy.getFirstStrategy();
        IStrategy second = strategy.getSecondStrategy();

        if (first instanceof IInputStrategyDescriptor) {
            if (second instanceof IInputStrategyDescriptor) {
                combineInputs(inputsToSpread, strategy);
            } else {
                getInputsCombination((ICombineStrategy) second, inputsToSpread);
            }
        } else if (second instanceof InputStrategy) {
            getInputsCombination((ICombineStrategy) first, inputsToSpread);
        } else {
            getInputsCombination((ICombineStrategy) first, inputsToSpread);
            getInputsCombination((ICombineStrategy) second, inputsToSpread);
        }
    }

    private static void combineInputs(Map<String, List<String>> inputsToSpread,
                                      ICombineStrategy strategy) throws EngineException {
        IInputStrategyDescriptor firstStrategy = (IInputStrategyDescriptor) strategy.getFirstStrategy();
        IInputStrategyDescriptor secondStrategy = (IInputStrategyDescriptor) strategy.getSecondStrategy();
        if (strategy instanceof IOneToOneStrategyDescriptor) {
            ValidateUtils.validateInputValues(inputsToSpread, firstStrategy.getInputName(), secondStrategy.getInputName());
        } else if (strategy instanceof IOneToManyStrategyDescriptor) {
            setOneToMany(firstStrategy.getInputName(), secondStrategy.getInputName(), inputsToSpread);
        }
    }

    private static void setOneToMany(String one, String many, Map<String, List<String>> inputsToSpread) {
        Collection<String> oneValues = inputsToSpread.get(one);
        Collection<String> manyValues = inputsToSpread.get(many);
        inputsToSpread.remove(one);
        inputsToSpread.remove(many);

        List<String> oneCombinedValues = new LinkedList<>();
        List<String> manyCombinedValues = new LinkedList<>();

        for (String oneVal : oneValues) {
            for (String manyVal : manyValues) {
                oneCombinedValues.add(oneVal);
                manyCombinedValues.add(manyVal);
            }
        }

        inputsToSpread.put(one, oneCombinedValues);
        inputsToSpread.put(many, manyCombinedValues);
    }
}
