package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.entities.contexts.StepContext;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;

import java.util.*;
import java.util.stream.Collectors;

public class TopologicSorter {

    public static Collection<ExecutionNode> parallelSort(PipelineContext pipeline) {
        Collection<IStepDescriptor> steps = getSteps(pipeline.getStepsContexts());
        Map<String, Collection<IStepDescriptor>> chainsFrom = getChainFrom(steps);
        Map<String, Collection<IStepDescriptor>> chainsTo = getChainTo(steps);
        Map<String, Collection<IStepDescriptor>> chainsToCpy = getChainTo(steps);
        List<IStepDescriptor> roots = getRoots(steps);
        Collection<ExecutionNode> graph = getRootJobs(pipeline, roots);

        while(!roots.isEmpty()) {
            IStepDescriptor root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId())) {
                addStepParents(graph, chainsToCpy, pipeline);
                return graph;
            }

            for(IStepDescriptor step : chainsFrom.get(root.getId())){
                if(chainsTo.containsKey(step.getId()))
                    chainsTo.get(step.getId()).remove(root);

                if(chainsTo.get(step.getId()).isEmpty()) {
                    roots.add(0, step);
                }
                addToGraphIfAbsent(graph, pipeline.getStepsContexts().get(root.getId()),
                                    pipeline.getStepsContexts().get(step.getId()));
            }
        }
        addStepParents(graph, chainsTo, pipeline);
        return graph;
    }

    public static Collection<ExecutionNode> sequentialSort(PipelineContext pipeline) {
        Collection<IStepDescriptor> steps = getSteps(pipeline.getStepsContexts());
        Map<String, Collection<IStepDescriptor>> chainsFrom = getChainFrom(steps);
        Map<String, Collection<IStepDescriptor>> chainsTo = getChainTo(steps);
        List<IStepDescriptor> roots = getRoots(steps);
        List<ExecutionNode> orderedSteps = new LinkedList<>(getRootJobs(pipeline, roots));

        while(!roots.isEmpty()) {
            IStepDescriptor root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId())) {
                addStepParents(orderedSteps);
                return orderedSteps;
            }

            for(IStepDescriptor step : chainsFrom.get(root.getId())){
                String id = step.getId();
                if(chainsTo.containsKey(id))
                    chainsTo.get(id).remove(root);

                if(chainsTo.get(id).isEmpty()) {
                    roots.add(0, step);
                    StepContext stepContext = pipeline.getStepsContexts().get(id);
                    if(notContainsNode(stepContext, orderedSteps)) {
                        orderedSteps.add(new ExecutionNode(stepContext));
                    }
                }
            }
        }

        addStepParents(orderedSteps);
        return orderedSteps;
    }



    private static Collection<IStepDescriptor> getSteps(Map<String, StepContext> stepsContexts) {
        Collection<IStepDescriptor> steps = new LinkedList<>();

        for (Map.Entry<String, StepContext> stepCtx : stepsContexts.entrySet())
            steps.add(stepCtx.getValue().getStep());

        return steps;
    }

    private static void addStepParents(List<ExecutionNode> orderedSteps) {

        for (int idx = orderedSteps.size() - 1; idx > 0; idx--) {
            LinkedList<StepContext> parents = new LinkedList<>();
            parents.add(orderedSteps.get(idx-1).getStepContext());
            orderedSteps.get(idx).getStepContext().getParents().addAll(parents);
        }
    }

    private static void addStepParents(Collection<ExecutionNode> graph, Map<String, Collection<IStepDescriptor>> chainsTo, PipelineContext pipeline) {
        for (ExecutionNode node : graph) {
            getParents(chainsTo, node, pipeline);
        }
    }

    private static void getParents(Map<String, Collection<IStepDescriptor>> chainsTo, ExecutionNode node, PipelineContext pipeline) {
        for (ExecutionNode child : node.getChilds()) {
            String stepId = child.getStepContext().getId();
            if (chainsTo.containsKey(stepId)) {
                Collection<StepContext> parents = new LinkedList<>();
                parents.addAll(getParents(chainsTo.get(stepId), pipeline));
                child.getStepContext().getParents().addAll(parents);
                chainsTo.remove(stepId);
            }

            getParents(chainsTo, child, pipeline);
        }
    }

    private static Collection<StepContext> getParents(Collection<IStepDescriptor> stepDescs, PipelineContext pipeline) {
        return stepDescs.stream()
                .map((s) -> pipeline.getStepsContexts().get(s.getId()))
                .collect(Collectors.toList());
    }

    private static void addToGraphIfAbsent(Collection<ExecutionNode> graph, StepContext root, StepContext child) {
        for (ExecutionNode executionNode : graph) {
            if (executionNode.getStepContext().equals(root)) {
                if (notContainsNode(child, executionNode.getChilds()))
                    executionNode.getChilds().add(new ExecutionNode(child));
            } else {
                addToGraphIfAbsent(executionNode.getChilds(), root, child);
            }
        }
    }

    private static boolean notContainsNode(StepContext child, Collection<ExecutionNode> graph) {
        return graph.stream()
                    .noneMatch((e) -> e.getStepContext().equals(child));
    }

    private static Collection<ExecutionNode> getRootJobs(PipelineContext pipeline, List<IStepDescriptor> roots) {
        Collection<ExecutionNode> executionNodes = new LinkedList<>();

        for (IStepDescriptor step : roots) {
            ExecutionNode executionNode = new ExecutionNode(pipeline.getStepsContexts().get(step.getId()));
            executionNodes.add(executionNode);
        }

        return executionNodes;
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
                    chainTo.computeIfAbsent(stepName, k -> new LinkedList<>());
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
                    chainFrom.computeIfAbsent(stepName, k -> new LinkedList<>());
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
