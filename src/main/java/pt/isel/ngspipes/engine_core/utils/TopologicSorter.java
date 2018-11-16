package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.StateEnum;
import pt.isel.ngspipes.engine_core.entities.contexts.Input;
import pt.isel.ngspipes.engine_core.entities.contexts.Job;
import pt.isel.ngspipes.engine_core.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_core.entities.contexts.Spread;
import pt.isel.ngspipes.engine_core.exception.EngineException;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class TopologicSorter {

    public static Collection<ExecutionNode> parallelSort(Pipeline pipeline) {
        List<Job> jobs = pipeline.getJobs();
        List<Job> roots = getRoots(jobs);
        Map<String, Collection<Job>> chainsFrom = getChainFrom(jobs);
        return parallelSort(pipeline, roots, chainsFrom);
    }

    public static Collection<ExecutionNode> parallelSort(Pipeline pipeline, Job job) {
        List<Job> jobs = pipeline.getJobs();
        Map<String, Collection<Job>> chainsFrom = getChainFrom(jobs);
        List<Job> roots = getParallelRoots(job, chainsFrom);
        return parallelSort(pipeline, roots, chainsFrom);
    }

    public static Collection<ExecutionNode> sequentialSort(Pipeline pipeline) {
        Collection<Job> jobs = pipeline.getJobs();
        Map<String, Collection<Job>> chainsFrom = getChainFrom(jobs);
        Map<String, Collection<Job>> chainsTo = getChainTo(jobs);
        List<Job> roots = getRoots(jobs);
        List<ExecutionNode> orderedSteps = getSequentialRoots(getRootJobs(roots));

        while(!roots.isEmpty()) {
            Job root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId())) {
                if (roots.size() == 0) {
                    return orderedSteps;
                } else {
                    continue;
                }
            }
            for (Job job : chainsFrom.get(root.getId())) {
                String id = job.getId();
                if (chainsTo.containsKey(id))
                    chainsTo.get(id).remove(root);

                if (chainsTo.get(id).isEmpty()) {
                    roots.add(0, job);
                    addSequentialChild(orderedSteps, job);
                }
            }
        }

        return orderedSteps;
    }



    private static Collection<ExecutionNode> parallelSort(Pipeline pipeline, List<Job> roots, Map<String, Collection<Job>> chainsFrom) {
        Collection<Job> jobs = pipeline.getJobs();
        Map<String, Collection<Job>> chainsTo = getChainTo(jobs);
        Map<String, Collection<Job>> chainsToCpy = getChainTo(jobs);
        Collection<ExecutionNode> graph = getRootJobs(roots);

        while(!roots.isEmpty()) {
            Job root = roots.remove(0);

            if (!root.isInconclusive()) {
                String rootID = root.getId();
                if (!chainsFrom.containsKey(rootID)) {
                    if (roots.size() == 0) {
                        addJobParents(graph, chainsToCpy, pipeline);
                        return graph;
                    } else {
                        continue;
                    }
                }

                for (Job job : chainsFrom.get(rootID)) {
                    if (chainsTo.containsKey(job.getId()))
                        chainsTo.get(job.getId()).remove(root);

                    if (chainsTo.get(job.getId()).isEmpty()) {
                        roots.add(job);
                    }

                    if (job.getSpread() != null){
                        root.setInconclusive(true);
                        setInconclusive(chainsFrom, rootID, job);
                    } else {
                        if (root.getSpread() == null)
                            addToGraphIfAbsent(graph, root, job, pipeline);
                        else {
                            root.setInconclusive(!root.isInconclusive());
                            setInconclusive(chainsFrom, job.getId(), job);
                        }
                    }
                }
            }
        }
        addJobParents(graph, chainsTo, pipeline);
        return graph;
    }

    private static void setInconclusive(Map<String, Collection<Job>> chainsFrom, String rootID, Job job) {
        job.setInconclusive(true);
        setInconclusive(chainsFrom, rootID);
    }

    private static void setInconclusive(Map<String, Collection<Job>> chainsFrom, String rootID) {
        if (chainsFrom.containsKey(rootID)) {
            for (Job child : chainsFrom.get(rootID)) {
                child.setInconclusive(!child.isInconclusive());
            }
        }
    }

    private static List<Job> getParallelRoots(Job job, Map<String, Collection<Job>> chainsFrom) {
        List<Job> roots = new LinkedList<>();
//
//        for (ExecutionNode node : rootJobs) {
//            Job job = node.getJob();
//            if (job.getState().getState().equals(StateEnum.SUCCESS) &&
//                    chainsFrom.containsKey(job.getId()) &&
//                    job.isInconclusive()) {
//                roots.addAll(getChildRoots(chainsFrom.get(job.getId()), chainsFrom));
//            }
//        }

        if (!chainsFrom.containsKey(job)) {
            return roots;
        }

        for (Job currJob : chainsFrom.get(job)) {
            if (currJob.isInconclusive()) {
                currJob.setInconclusive(!currJob.isInconclusive());
                roots.add(currJob);
            }
        }

        return roots;
    }

    private static List<ExecutionNode> getChildRoots(Collection<Job> jobs, Map<String, Collection<Job>> chainsFrom) {
        List<ExecutionNode> roots = new LinkedList<>();

        for (Job job : jobs) {
            if (!job.getState().getState().equals(StateEnum.SUCCESS)) {
                roots.add(new ExecutionNode(job));
                job.setInconclusive(!job.isInconclusive());
            } else if (chainsFrom.containsKey(job.getId())) {
                roots.addAll(getChildRoots(chainsFrom.get(job.getId()), chainsFrom));
            }
        }
        return roots;
    }

    private static List<ExecutionNode> getSequentialRoots(List<ExecutionNode> rootJobs) {
        List<ExecutionNode> nodes = new LinkedList<>();
        nodes.add(rootJobs.get(0));

        for (int idx = 1; idx < rootJobs.size(); idx++)
            addSequentialChild(nodes, rootJobs.get(idx).getJob());

        return nodes;
    }

    private static void addSequentialChild(List<ExecutionNode> orderedSteps, Job job) {
        ExecutionNode node = orderedSteps.get(0);
        if (!node.getJob().getId().equals(job.getId())) {
            if (node.getChilds().isEmpty()) {
                node.getChilds().add(new ExecutionNode(job));
                job.addParent(node.getJob());
            } else
                addSequentialChild(node.getChilds(), job);
        }
    }

    private static void addJobParents(Collection<ExecutionNode> graph, Map<String, Collection<Job>> chainsTo, Pipeline pipeline) {
        for (ExecutionNode node : graph) {
            getParents(chainsTo, node, pipeline);
        }
    }

    private static void getParents(Map<String, Collection<Job>> chainsTo, ExecutionNode node, Pipeline pipeline) {
        for (ExecutionNode child : node.getChilds()) {
            String jobId = child.getJob().getId();
            if (chainsTo.containsKey(jobId)) {
                Collection<String> parents = new LinkedList<>();
                parents.addAll(getParents(chainsTo.get(jobId), pipeline));
                child.getJob().getParents().addAll(parents);
                chainsTo.remove(jobId);
            }

            getParents(chainsTo, child, pipeline);
        }
    }

    private static Collection<String> getParents(Collection<Job> stepDescs, Pipeline pipeline) {
        return stepDescs.stream()
                .map((s) -> pipeline.getJobById(s.getId()).getId())
                .collect(Collectors.toList());
    }

    private static void addToGraphIfAbsent(Collection<ExecutionNode> graph, Job root, Job child, Pipeline pipeline) {
        for (ExecutionNode executionNode : graph) {
            if (executionNode.getJob().equals(root)) {
                if (notContainsNode(child, executionNode.getChilds())) {
                    executionNode.getChilds().add(new ExecutionNode(child));
                }
            } else {
                addToGraphIfAbsent(executionNode.getChilds(), root, child, pipeline);
            }
        }
    }

    private static boolean notContainsNode(Job child, Collection<ExecutionNode> graph) {
        return graph.stream()
                .noneMatch((e) -> e.getJob().equals(child));
    }

    private static List<ExecutionNode> getRootJobs(List<Job> roots) {
        List<ExecutionNode> executionNodes = new LinkedList<>();

        for (Job job : roots) {
            ExecutionNode executionNode = new ExecutionNode(job);
            executionNodes.add(executionNode);
        }

        return executionNodes;
    }

    private static List<Job> getRoots(Collection<Job> jobs) {
        List<Job> rootJobs = new LinkedList<>();
        for (Job job : jobs) {
            Collection<Input> chainInputs = getChainInputs(job);
            if (chainInputs.isEmpty())
                rootJobs.add(job);
        }
        return rootJobs;
    }

    private static Map<String, Collection<Job>> getChainTo(Collection<Job> jobs) {
        Map<String, Collection<Job>> chainTo = new HashMap<>();
        for (Job jobInside : jobs) {
            Collection<Input> chainInputs = getChainInputs(jobInside);
            if (chainInputs.isEmpty())
                continue;

            for (Job job : jobs) {
                String jobName = jobInside.getId();
                if (jobName.equals(job.getId()))
                    continue;
                if(isChainStep(job.getId(), chainInputs)) {
                    chainTo.computeIfAbsent(jobName, k -> new LinkedList<>());
                    chainTo.get(jobName).add(job);
                }
            }
        }
        return chainTo;
    }

    private static Map<String, Collection<Job>> getChainFrom(Collection<Job> jobs) {
        Map<String, Collection<Job>> chainFrom = new HashMap<>();
        for (Job job : jobs) {
            String jobName = job.getId();
            for (Job jobInside : jobs) {
                if (jobName.equals(jobInside.getId()))
                    continue;
                Collection<Input> chainInputs = getChainInputs(jobInside);
                if (chainInputs.isEmpty())
                    continue;
                if(isChainStep(jobName, chainInputs)) {
                    chainFrom.computeIfAbsent(jobName, k -> new LinkedList<>());
                    chainFrom.get(jobName).add(jobInside);
                }
            }

        }
        return chainFrom;
    }

    private static boolean isChainStep(String stepName, Collection<Input> chainInputs) {
        for (Input input : chainInputs) {
            if(input.getOriginStep() != null && input.getOriginStep().equals(stepName))
                return true;
        }
        return false;
    }

    private static Collection<Input> getChainInputs(Job job) {
        Collection<Input> chainInputs = new LinkedList<>();
        for (Input input : job.getInputs())
            if (input.getOriginStep() != null && !input.getOriginStep().equals(job.getId()))
                chainInputs.add(input);
        return chainInputs;
    }
}
