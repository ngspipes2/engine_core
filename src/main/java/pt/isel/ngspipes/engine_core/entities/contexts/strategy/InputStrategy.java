package pt.isel.ngspipes.engine_core.entities.contexts.strategy;

public class InputStrategy extends Strategy {

    private String inputName;
    public String getInputName() { return this.inputName; }
    public void setInputName(String inputName) { this.inputName = inputName; }


    public InputStrategy(String inputName) {
        this.inputName = inputName;
    }

    public InputStrategy() { }

}
