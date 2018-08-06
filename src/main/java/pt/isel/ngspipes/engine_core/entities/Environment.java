package pt.isel.ngspipes.engine_core.entities;

public class Environment {

    String workDirectory;
    String outputsDirectory;
    int memory;
    int cpu;
    int disk;

    public String getWorkDirectory() { return workDirectory; }
    public void setWorkDirectory(String workDirectory) { this.workDirectory = workDirectory; }

    public String getOutputsDirectory() { return outputsDirectory; }
    public void setOutputsDirectory(String outputsDirectory) { this.outputsDirectory = outputsDirectory; }

    public int getMemory() { return memory; }
    public void setMemory(int memory) { this.memory = memory; }

    public int getCpu() { return cpu; }
    public void setCpu(int cpu) { this.cpu = cpu; }

    public int getDisk() { return disk; }
    public void setDisk(int disk) { this.disk = disk; }

}
