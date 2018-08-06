package pt.isel.ngspipes.engine_core.executionReporter;

import pt.isel.ngspipes.engine_core.exception.ProgressReporterException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class FileReporter  implements IExecutionProgressReporter {

    public static final String TRACE_TAG = "TRACE\t";
    public static final String ERROR_TAG = "ERROR\t";
    public static final String INFO_TAG = "INFO\t";

    private final String path;
    private PrintWriter writer;

    public FileReporter(String path){
        this.path = path;
    }

    @Override
    public void reportTrace(String msg) {
        report(TRACE_TAG + msg);
    }

    @Override
    public void reportError(String msg) {
        report(ERROR_TAG + msg);
    }

    @Override
    public void reportInfo(String msg) {
        report(INFO_TAG + msg);
    }

    private void report(String msg) {
        if(writer != null){
            writer.println(msg);
            writer.flush();
        }
    }

    @Override
    public void open() throws ProgressReporterException {
        try {
            writer = new PrintWriter(path);
        } catch (FileNotFoundException ex) {
            throw new ProgressReporterException("File " + path + " not found!", ex);
        }
    }

    @Override
    public void close() {
        writer.close();
    }
}
