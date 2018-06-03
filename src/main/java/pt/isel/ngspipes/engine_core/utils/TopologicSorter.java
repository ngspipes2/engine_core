package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;

import java.util.*;

public class TopologicSorter {

    public static Collection<ExecutionNode> sort(Map<String, IStepDescriptor> steps) {
        Collection<IStepDescriptor> values = steps.values();
        Map<String, Collection<IStepDescriptor>> chainsFrom = getChainFrom(values);
        Map<String, Collection<IStepDescriptor>> chainsTo = getChainTo(values);
        List<IStepDescriptor> roots = getRoots(values);
        Collection<ExecutionNode> graph = getInitialGraph(roots, steps);

        while(!roots.isEmpty()) {
            IStepDescriptor root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId()))
                return graph;

            for(IStepDescriptor step : chainsFrom.get(root.getId())){
                if(chainsTo.containsKey(step.getId()))
                    chainsTo.get(step.getId()).remove(root);

                if(chainsTo.get(step.getId()).isEmpty()) {
                    roots.add(0, step);
                }
                addToGraphIfAbsent(graph, root.getId(), step.getId());
            }
        }

        return graph;
    }

    private static void addToGraphIfAbsent(Collection<ExecutionNode> graph, String rootId, String childId) {
        for (ExecutionNode executionNode : graph) {
            if (executionNode.getId().equals(rootId)) {
                if (!executionNode.getChilds().contains(childId))
                    executionNode.getChilds().add(new ExecutionNode(childId));
            } else {
                addToGraphIfAbsent(executionNode.getChilds(), rootId, childId);
            }
        }
    }

    private static Collection<ExecutionNode> getInitialGraph(List<IStepDescriptor> roots, Map<String, IStepDescriptor> steps) {
        Collection<ExecutionNode> graph = new LinkedList<>();

        for (Map.Entry<String, IStepDescriptor> step : steps.entrySet())
            if (roots.contains(step.getValue()))
                graph.add(new ExecutionNode(step.getKey()));
        return graph;
    }

    public static Collection<IStepDescriptor> sort(Collection<IStepDescriptor> steps) {
        Map<String, Collection<IStepDescriptor>> chainsFrom = getChainFrom(steps);
        Map<String, Collection<IStepDescriptor>> chainsTo = getChainTo(steps);
        List<IStepDescriptor> roots = getRoots(steps);
        List<IStepDescriptor> orderedSteps = new LinkedList<>(roots);

        while(!roots.isEmpty()) {
            IStepDescriptor root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId()))
                return orderedSteps;

            for(IStepDescriptor step : chainsFrom.get(root.getId())){
                if(chainsTo.containsKey(step.getId()))
                    chainsTo.get(step.getId()).remove(root);

                if(chainsTo.get(step.getId()).isEmpty()) {
                    roots.add(0, step);
                    if(!orderedSteps.contains(step))
                        orderedSteps.add(step);
                }
            }
        }

        return orderedSteps;
    }



    private static List<IStepDescriptor> getRoots(Collection<IStepDescriptor> steps) {
        List<IStepDescriptor> rootSteps = new LinkedList<>();
        for (IStepDescriptor step : steps) {
            Collection<ChainInputDescriptor> chainInputs = getChainInputs(step);
            if (chainInputs.isEmpty())
                rootSteps.add(step);
        }
        return rootSteps;
    }

    private static Map<String, Collection<IStepDescriptor>> getChainTo(Collection<IStepDescriptor> steps) {
        Map<String, Collection<IStepDescriptor>> chainTo = new HashMap<>();
        for (IStepDescriptor step : steps) {
            for (IStepDescriptor stepInside : steps) {
                String stepName = stepInside.getId();
                if (stepName.equals(step.getId()))
                    continue;
                Collection<ChainInputDescriptor> chainInputs = getChainInputs(stepInside);
                if (chainInputs.isEmpty())
                    continue;
                if(isChainStep(step.getId(), chainInputs)) {
                    if (chainTo.get(stepName) == null)
                        chainTo.put(stepName, new LinkedList<>());
                    chainTo.get(stepName).add(step);
                }
            }
        }
        return chainTo;
    }

    private static Map<String, Collection<IStepDescriptor>> getChainFrom(Collection<IStepDescriptor> steps) {
        Map<String, Collection<IStepDescriptor>> chainFrom = new HashMap<>();
        for (IStepDescriptor step : steps) {
            String stepName = step.getId();
            for (IStepDescriptor stepInside : steps) {
                if (stepName.equals(stepInside.getId()))
                    continue;
                Collection<ChainInputDescriptor> chainInputs = getChainInputs(stepInside);
                if (chainInputs.isEmpty())
                    continue;
                if(isChainStep(stepName, chainInputs)) {
                    if (chainFrom.get(stepName) == null)
                        chainFrom.put(stepName, new LinkedList<>());
                    chainFrom.get(stepName).add(stepInside);
                }
            }

        }
        return chainFrom;
    }

    private static boolean isChainStep(String stepName, Collection<ChainInputDescriptor> chainInputs) {
        for (ChainInputDescriptor input : chainInputs) {
            if(input.getStepId().equals(stepName))
                return true;
        }
        return false;
    }

    private static Collection<ChainInputDescriptor> getChainInputs(IStepDescriptor step) {
        Collection<ChainInputDescriptor> chainInputs = new LinkedList<>();
        for (IInputDescriptor input : step.getInputs())
            if (input instanceof ChainInputDescriptor)
                chainInputs.add((ChainInputDescriptor) input);
        return chainInputs;
    }
}
