package pt.isel.ngspipes.engine_core.entities.contexts;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class Output {

    private String name;
    private String originJob;
    private String type;
    private Object value;

    @JsonIgnore
    private Job originJobRef;

    @JsonIgnore
    private List<String> usedBy;

    public Output(String name, Job originJob, String type, Object value) {
        this.name = name;
        this.originJob = originJob.getId();
        this.originJobRef = originJob;
        this.value = value;
        this.type = type;
    }

    public Output() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getOriginJob() { return originJobRef.getId(); }
    public void setOriginJob(String originJob) { this.originJob = originJob; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Object getValue() { return value; }
    public void setValue(Object value) { this.value = value; }

    public List<String> getUsedBy() { return usedBy; }
    public void setUsedBy(List<String> usedBy) { this.usedBy = usedBy; }

    public void setOriginJobRef(Job originJobRef) {
        this.originJobRef = originJobRef;
        this.originJob = originJobRef.getId();
    }
}
