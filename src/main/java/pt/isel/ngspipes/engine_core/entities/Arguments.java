package pt.isel.ngspipes.engine_core.entities;

public class Arguments {


    public String outPath;
    public int cpus;
    public int mem;
    public int disk;
    public boolean parallel;

    public Arguments() {
    }

    public Arguments(String outPath, boolean parallel) {
        this.outPath = outPath;
        this.parallel = parallel;
    }

    public Arguments(String outPath, int cpus, int mem, int disk, boolean parallel) {
        this(outPath, parallel);
        this.outPath = outPath;
        this.cpus = cpus;
        this.mem = mem;
        this.disk = disk;
    }
}
