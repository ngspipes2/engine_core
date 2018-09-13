package pt.isel.ngspipes.engine_core.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class IOUtils {

    public static void copyFile(String source, String dest) throws IOException {
        Path sourcePath = Paths.get(source);
        Path destPath = Paths.get(dest);
        File destFile = new File(destPath.toString());
        destFile.getParentFile().mkdirs();
        destFile.createNewFile();
        Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);

    }

    public static void createFolder(String folderPath) {
        File destFile = new File(folderPath);
        destFile.mkdirs();
    }
}
