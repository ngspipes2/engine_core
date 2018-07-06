package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.JobUnit;
import pt.isel.ngspipes.engine_core.entities.Pipeline;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;

import java.util.*;

public class TopologicSorter {

    public static Collection<ExecutionNode> parallelSort(Pipeline pipeline) {
        Collection<IStepDescriptor> steps = pipeline.getSteps().values();
        Map<String, Collection<IStepDescriptor>> chainsFrom = getChainFrom(steps);
        Map<String, Collection<IStepDescriptor>> chainsTo = getChainTo(steps);
        Map<String, Collection<IStepDescriptor>> chainsToCpy = getChainTo(steps);
        List<IStepDescriptor> roots = getRoots(steps);
        Collection<ExecutionNode> graph = getRootJobs(pipeline, roots);

        while(!roots.isEmpty()) {
            IStepDescriptor root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId())) {
                addJobUnitParents(graph, chainsToCpy, pipeline);
                return graph;
            }

            for(IStepDescriptor step : chainsFrom.get(root.getId())){
                if(chainsTo.containsKey(step.getId()))
                    chainsTo.get(step.getId()).remove(root);

                if(chainsTo.get(step.getId()).isEmpty()) {
                    roots.add(0, step);
                }
                addToGraphIfAbsent(graph, pipeline.getJobUnits().get(root.getId()),
                                    pipeline.getJobUnits().get(step.getId()));
            }
        }
        addJobUnitParents(graph, chainsTo, pipeline);
        return graph;
    }

    public static Collection<ExecutionNode> sequentialSort(Pipeline pipeline) {
        Collection<IStepDescriptor> steps = pipeline.getSteps().values();
        Map<String, Collection<IStepDescriptor>> chainsFrom = getChainFrom(steps);
        Map<String, Collection<IStepDescriptor>> chainsTo = getChainTo(steps);
        List<IStepDescriptor> roots = getRoots(steps);
        List<ExecutionNode> orderedSteps = new LinkedList<>(getRootJobs(pipeline, roots));

        while(!roots.isEmpty()) {
            IStepDescriptor root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId())) {
                addJobUnitParents(orderedSteps);
                return orderedSteps;
            }

            for(IStepDescriptor step : chainsFrom.get(root.getId())){
                if(chainsTo.containsKey(step.getId()))
                    chainsTo.get(step.getId()).remove(root);

                if(chainsTo.get(step.getId()).isEmpty()) {
                    roots.add(0, step);
                    JobUnit jobUnit = pipeline.getJobUnits().get(step.getId());
                    if(!orderedSteps.contains(jobUnit))
                        orderedSteps.add(new ExecutionNode(jobUnit));
                }
            }
        }

        addJobUnitParents(orderedSteps);
        return orderedSteps;
    }

    private static void addJobUnitParents(List<ExecutionNode> orderedSteps) {

        for (int idx = orderedSteps.size() - 1; idx > 0; idx--) {
            LinkedList<JobUnit> parents = new LinkedList<>();
            parents.add(orderedSteps.get(idx-1).getJob());
            orderedSteps.get(idx).getJob().setParents(parents);
        }
    }

    private static void addJobUnitParents(Collection<ExecutionNode> graph,
                                          Map<String, Collection<IStepDescriptor>> chainsTo, Pipeline pipeline) {
        for (ExecutionNode node : graph) {
            getParents(chainsTo, pipeline, node);
        }
    }

    private static void getParents(Map<String, Collection<IStepDescriptor>> chainsTo, Pipeline pipeline, ExecutionNode node) {
        for (ExecutionNode child : node.getChilds()) {
            Collection<JobUnit> parents = new LinkedList<>();
            String jobId = child.getJob().getId();
            parents.addAll(getParents(chainsTo.get(jobId), pipeline));
            child.getJob().setParents(parents);
            getParents(chainsTo, pipeline, child);
        }
    }

    private static Collection<JobUnit> getParents(Collection<IStepDescriptor> stepDescs, Pipeline pipeline) {
        Collection<JobUnit> parents = new LinkedList<>();

        for (IStepDescriptor step : stepDescs) {
            parents.add(pipeline.getJobUnits().get(step.getId()));
        }

        return parents;
    }

    private static void addToGraphIfAbsent(Collection<ExecutionNode> graph, JobUnit root, JobUnit child) {
        for (ExecutionNode executionNode : graph) {
            if (executionNode.getJob().equals(root)) {
                if (!executionNode.getChilds().contains(child))
                    executionNode.getChilds().add(new ExecutionNode(child));
            } else {
                addToGraphIfAbsent(executionNode.getChilds(), root, child);
            }
        }
    }

    private static Collection<ExecutionNode> getRootJobs(Pipeline pipeline, List<IStepDescriptor> roots) {
        Collection<ExecutionNode> executionNodes = new LinkedList<>();

        for (IStepDescriptor step : roots) {
            ExecutionNode executionNode = new ExecutionNode(pipeline.getJobUnits().get(step.getId()));
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
