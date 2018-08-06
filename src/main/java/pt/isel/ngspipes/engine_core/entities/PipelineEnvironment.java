package pt.isel.ngspipes.engine_core.entities;

import java.util.Collection;
import java.util.Map;

public class PipelineEnvironment extends Environment {

    Map<String, Collection<String>> inputs;

    public PipelineEnvironment() {
    }

    public Map<String, Collection<String>> getInputs() { return inputs; }
    public void setInputs(Map<String, Collection<String>> inputs) { this.inputs = inputs; }
}
