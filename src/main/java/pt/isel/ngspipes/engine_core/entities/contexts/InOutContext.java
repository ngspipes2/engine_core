package pt.isel.ngspipes.engine_core.entities.contexts;

public class InOutContext {

    private final String name;
    private final String originStep;
    private final String type;
    private final Object value;
    private String usedBy;

    public InOutContext(String name, String originStep, String type, Object value) {
        this.name = name;
        this.originStep = originStep;
        this.value = value;
        this.type = type;
    }

    public String getOriginStep() { return originStep; }
    public String getName() { return name; }
    public String getType() { return type; }
    public Object getValue() { return value; }

    public String getUsedBy() { return usedBy; }
    public void setUsedBy(String usedBy) { this.usedBy = usedBy; }
}
