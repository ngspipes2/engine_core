package pt.isel.ngspipes.engine_core.entities;

import java.util.Collection;

public class JobUnit {

    String id;
    Collection<JobUnit> parents;
    int recommendedMemory;
    int recommendedCPU;
    int recommendedDisk;
    Collection<String> inputs;
    Collection<String> outputs;
    String outputDirectory;
    String workingDirectory;
    ExecutionState state;

    public JobUnit(String id, int recommendedMemory, int recommendedCPU, int recommendedDisk, Collection<String> inputs,
                   Collection<String> outputs, String outputDirectory, String workingDirectory, ExecutionState state) {
        this.id = id;
        this.recommendedMemory = recommendedMemory;
        this.recommendedCPU = recommendedCPU;
        this.recommendedDisk = recommendedDisk;
        this.inputs = inputs;
        this.outputs = outputs;
        this.outputDirectory = outputDirectory;
        this.workingDirectory = workingDirectory;
        this.state = state;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Collection<JobUnit> getParents() { return parents; }
    public void setParents(Collection<JobUnit> parents) { this.parents = parents; }

    public int getRecommendedMemory() { return recommendedMemory; }
    public void setRecommendedMemory(int recommendedMemory) { this.recommendedMemory = recommendedMemory; }

    public int getRecommendedCPU() { return recommendedCPU; }
    public void setRecommendedCPU(int recommendedCPU) { this.recommendedCPU = recommendedCPU; }

    public int getRecommendedDisk() { return recommendedDisk; }
    public void setRecommendedDisk(int recommendedDisk) { this.recommendedDisk = recommendedDisk; }

    public Collection<String> getInputs() { return inputs; }
    public void setInputs(Collection<String> inputs) { this.inputs = inputs; }

    public Collection<String> getOutputs() { return outputs; }
    public void setOutputs(Collection<String> outputs) { this.outputs = outputs; }

    public String getOutputDirectory() { return outputDirectory; }
    public void setOutputDirectory(String outputDirectory) { this.outputDirectory = outputDirectory; }

    public String getWorkingDirectory() { return workingDirectory; }
    public void setWorkingDirectory(String workingDirectory) { this.workingDirectory = workingDirectory; }

    public ExecutionState getState() { return state; }
    public void setState(ExecutionState state) { this.state = state; }
}
