package pt.isel.ngspipes.engine_core.executionReporter;

import pt.isel.ngspipes.engine_core.exception.ProgressReporterException;

public interface IExecutionProgressReporter {

    void open() throws ProgressReporterException;
    void reportTrace(String msg) throws ProgressReporterException;
    void reportError(String msg) throws ProgressReporterException;
    void reportInfo(String msg) throws ProgressReporterException;
    void close() throws ProgressReporterException;
}
