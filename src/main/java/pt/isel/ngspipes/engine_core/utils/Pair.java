package pt.isel.ngspipes.engine_core.utils;

public class Pair<T, U> {

    T val1;
    U val2;

    public Pair(T val1, U val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public T getVal1() { return val1; }

    public U getVal2() { return val2; }
}
