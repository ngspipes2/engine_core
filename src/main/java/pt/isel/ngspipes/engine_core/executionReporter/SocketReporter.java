package pt.isel.ngspipes.engine_core.executionReporter;

import pt.isel.ngspipes.engine_core.exception.ProgressReporterException;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class SocketReporter implements IExecutionProgressReporter {

    public static final String TRACE_TAG = "TRACE\t";
    public static final String ERROR_TAG = "ERROR\t";
    public static final String INFO_TAG = "INFO\t";

    private final int port;
    private final String ipAddress;
    private PrintWriter writer;
    private Socket serverSocket;

    public SocketReporter(int port, String ipAddress) {
        this.port = port;
        this.ipAddress = ipAddress;
    }


    @Override
    public void reportTrace(String msg) throws ProgressReporterException {
        writer.println(TRACE_TAG + msg);
        writer.flush();
    }

    @Override
    public void reportError(String msg) throws ProgressReporterException {
        writer.println(ERROR_TAG + msg);
        writer.flush();
    }

    @Override
    public void reportInfo(String msg) throws ProgressReporterException {
        writer.println(INFO_TAG + msg);
        writer.flush();
    }

    @Override
    public void open() throws ProgressReporterException {

        InetAddress address;
        try {
            address = InetAddress.getByName(ipAddress);
            System.out.println(address.getHostAddress() + " : " + address.getHostName());
            serverSocket = new Socket(address, port);
            writer = new PrintWriter(serverSocket.getOutputStream(), true);
        } catch (IOException e) {
            throw new ProgressReporterException("Couldn't connect to host: " + ipAddress + " on port: "
                    + port, e);
        }
    }

    @Override
    public void close() throws ProgressReporterException {
        writer.close();
        try {
            if(!serverSocket.isClosed())
                serverSocket.close();
        } catch (IOException e) {
            throw new ProgressReporterException("Error closing socket in host: " + ipAddress + " and port: "
                    + port, e);
        }
    }
}
