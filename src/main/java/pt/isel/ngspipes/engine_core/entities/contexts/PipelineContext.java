package pt.isel.ngspipes.engine_core.entities.contexts;

import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.ExecutionState;
import pt.isel.ngspipes.engine_core.entities.PipelineEnvironment;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.util.Collection;
import java.util.Map;

public class PipelineContext {

    private String executionId;
    private String pipelineName;
    private IPipelineDescriptor descriptor;
    private Map<String, Object> parameters;

    private Map<String, StepContext> stepContexts;
    private PipelineEnvironment pipelineEnvironment;
    private Collection<ExecutionNode> pipelineGraph;
    private ExecutionState state;

    public PipelineContext(String executionId, Map<String, Object> parameters, Map<String, StepContext> stepContexts,
                           IPipelineDescriptor descriptor, PipelineEnvironment environment) {
        this.executionId = executionId;
        this.parameters = parameters;
        this.stepContexts = stepContexts;
        this.pipelineName = descriptor.getName();
        this.descriptor = descriptor;
        this.pipelineEnvironment = environment;
    }

    public PipelineContext(String executionId, Map<String, Object> parameters, Map<String, StepContext> stepContexts,
                           IPipelineDescriptor descriptor) {
        this(executionId, parameters, stepContexts, descriptor, new PipelineEnvironment());
    }

    public String getExecutionId() { return executionId; }
    public String getPipelineName() { return pipelineName; }
    public Map<String, StepContext> getStepsContexts() { return stepContexts; }
    public IPipelineDescriptor getDescriptor() { return descriptor; }
    public PipelineEnvironment getPipelineEnvironment() { return pipelineEnvironment; }
    public Map<String, Object> getParameters() { return parameters; }


    public Collection<ExecutionNode> getPipelineGraph() { return pipelineGraph; }
    public void setPipelineGraph(Collection<ExecutionNode> pipelineGraph) { this.pipelineGraph = pipelineGraph; }

    public ExecutionState getState() { return state; }
    public void setState(ExecutionState state) { this.state = state; }
}
