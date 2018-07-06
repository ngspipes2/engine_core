package pt.isel.ngspipes.engine_core.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class ProcessRunner {

    @FunctionalInterface
    private interface LogLambda {
        void log() throws Exception;
    }

    protected static void logStream(InputStream in) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));

        String line;
        StringBuilder sb = new StringBuilder();

        try{
            while((line=bf.readLine()) != null){
                sb.append(line).append("\n");
                System.out.println(line);
            }
        }finally{
            if(sb.length() != 0)
                System.out.println(sb.toString());

            bf.close();
        }
    }

    public static void run(String command) throws IOException {

        //Ignoring IOExceptions from logStream(InputStream in)
        int exitCode = 0;
        try {

            Process p = Runtime.getRuntime().exec(command);

            Thread inputThread = createThread(() -> logStream(p.getInputStream()));
            Thread errorThread = createThread(() -> logStream(p.getErrorStream()));

            inputThread.join();
            errorThread.join();

            exitCode = p.waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
        String message = "Finished with Exit Code = " + exitCode;
        System.out.println(message);
    }

    private static Thread createThread(LogLambda action) {
        Thread t = new Thread( () -> {
            try {
                action.log();
            } catch(Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        });

        t.setDaemon(true);
        t.start();

        return t;
    }
}
