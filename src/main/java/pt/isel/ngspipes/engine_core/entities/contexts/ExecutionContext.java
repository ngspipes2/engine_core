package pt.isel.ngspipes.engine_core.entities.contexts;

import java.util.Map;

public class ExecutionContext {

    String name;
    String context;
    Map<String, Object> config;

    public ExecutionContext(String name, String context, Map<String, Object> config) {
        this.name = name;
        this.context = context;
        this.config = config;
    }

    public ExecutionContext() {}

    public String getName() { return name; }
    public String getContext() { return context; }
    public Map<String, Object> getConfig() { return config; }
}
