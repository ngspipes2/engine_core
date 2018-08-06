package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.*;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public class SpreadCombiner {


    public static void getInputsCombination(ICombineStrategyDescriptor strategy, Map<String, Collection<String>> inputsToSpread) throws EngineException {

        IStrategyDescriptor first = strategy.getFirstStrategy();
        IStrategyDescriptor second = strategy.getSecondStrategy();

        if (first instanceof IInputStrategyDescriptor) {
            if (second instanceof IInputStrategyDescriptor) {
                combineInputs(inputsToSpread, strategy);
            } else {
                getInputsCombination((ICombineStrategyDescriptor) second, inputsToSpread);
            }
        } else if (second instanceof IInputStrategyDescriptor) {
            getInputsCombination((ICombineStrategyDescriptor) first, inputsToSpread);
        } else {
            getInputsCombination((ICombineStrategyDescriptor) first, inputsToSpread);
            getInputsCombination((ICombineStrategyDescriptor) second, inputsToSpread);
        }
    }

    private static void combineInputs(Map<String, Collection<String>> inputsToSpread,
                               ICombineStrategyDescriptor strategy) throws EngineException {
        IInputStrategyDescriptor firstStrategy = (IInputStrategyDescriptor) strategy.getFirstStrategy();
        IInputStrategyDescriptor secondStrategy = (IInputStrategyDescriptor) strategy.getSecondStrategy();
        if (strategy instanceof IOneToOneStrategyDescriptor) {
            ValidateUtils.validateInputValues(inputsToSpread, firstStrategy.getInputName(), secondStrategy.getInputName());
        } else if (strategy instanceof IOneToManyStrategyDescriptor) {
            setOneToMany(firstStrategy.getInputName(), secondStrategy.getInputName(), inputsToSpread);
        }
    }

    private static void setOneToMany(String one, String many, Map<String, Collection<String>> inputsToSpread) {
        Collection<String> oneValues = inputsToSpread.get(one);
        Collection<String> manyValues = inputsToSpread.get(many);
        inputsToSpread.remove(one);
        inputsToSpread.remove(many);

        Collection<String> oneCombinedValues = new LinkedList<>();
        Collection<String> manyCombinedValues = new LinkedList<>();

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
