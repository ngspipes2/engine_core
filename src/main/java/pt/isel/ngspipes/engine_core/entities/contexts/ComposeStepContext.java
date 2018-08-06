package pt.isel.ngspipes.engine_core.entities.contexts;

import pt.isel.ngspipes.engine_core.entities.Environment;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;

public class ComposeStepContext extends StepContext {

    IPipelineDescriptor pipelineDescriptor;

    public ComposeStepContext(String id, Environment environment, IStepDescriptor step, IPipelineDescriptor pipelineDescriptor) {
        super(id, environment, step);
        this.pipelineDescriptor = pipelineDescriptor;
    }

    public IPipelineDescriptor getPipelineDescriptor() { return pipelineDescriptor; }
}
