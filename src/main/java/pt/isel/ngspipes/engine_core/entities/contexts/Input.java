package pt.isel.ngspipes.engine_core.entities.contexts;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public class Input {

    private String name;
    private String originStep;
    private String chainOutput;
    private String type;
    private String value;
    private String prefix;
    private String separator;
    private String suffix;
    private List<Input> subInputs;

    @JsonIgnore
    private Job originJob;

    public Input(String name, Job originJob, String chainOutput, String type, String value, String prefix,
                 String separator, String suffix, List<Input> subInputs) {
        this(name, originJob.getId(), chainOutput, type, value);
        this.prefix = prefix;
        this.separator = separator;
        this.suffix = suffix;
        this.subInputs = subInputs;
        this.originJob = originJob;
        this.originStep = originJob.getId();
    }

    public Input(String name, String originStep, String chainOutput, String type, String value) {
        this.name = name;
        this.originStep = originStep;
        this.chainOutput = chainOutput;
        this.value = value;
        this.type = type;
    }

    public Input() {}

    public String getOriginStep() { return originJob.getId(); }
    public String getName() { return name; }
    public String getType() { return type; }
    public String getValue() { return value; }
    public String getChainOutput() { return chainOutput; }
    public String getPrefix() { return prefix; }
    public String getSeparator() { return separator; }
    public String getSuffix() { return suffix; }
    public List<Input> getSubInputs() { return subInputs; }

}