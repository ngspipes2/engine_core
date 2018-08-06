package pt.isel.ngspipes.engine_core.utils;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.IOException;

public class LoggerUtils {

    public static void initLogger(String filePath) {
        try {
            PatternLayout layout = new PatternLayout("%-5p %d %m%n");
            RollingFileAppender appender = new RollingFileAppender(layout, filePath);
            Logger.getRootLogger().addAppender(appender);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
