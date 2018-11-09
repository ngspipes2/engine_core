package pt.isel.ngspipes.engine_core.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class IOUtils {

    public static void copyFile(String source, String dest) throws IOException {
        if (source.contains("*")) {
            copyGlobFiles(source, dest);
        } else {
            copySimpleFile(source, dest);
        }
    }

    public static void createFolder(String folderPath) {
        File destFile = new File(folderPath);
        destFile.mkdirs();
    }

    public static void verifyFile(String path) throws IOException {
        File file = new File(path);

        if (!file.exists())
            throw new IOException("File " + path + " not found");
    }

    public static void copyDirectory(String source, String dest) throws IOException {

        Path sourcePath = Paths.get(source);
        Path destPath = Paths.get(dest);
        File destFile = new File(destPath.toString());
        destFile.getParentFile().mkdirs();
        destFile.createNewFile();
        Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);

        for (File file : Objects.requireNonNull(new File(sourcePath.toString()).listFiles())) {
            String fileName = File.separatorChar + file.getName();
            copyFile(source + fileName, dest + fileName);
        }
    }

    public static List<String> getFileNamesByPattern(String directoryPath, String pattern) {
        File dir = new File(directoryPath);
        List<String> names = new LinkedList<>();
        File [] files = dir.listFiles((dir1, name) -> name.matches(pattern));

        if (files != null) {
            for (File file : files)
                names.add(file.getName());
        }

        return names;
    }



    private static void copySimpleFile(String source, String dest) throws IOException {
        Path sourcePath = Paths.get(source);
        Path destPath = Paths.get(dest);
        File destFile = new File(destPath.toString());
        destFile.getParentFile().mkdirs();
        destFile.createNewFile();
        Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private static void copyGlobFiles(String source, String dest) throws IOException {
        String pattern = source.substring(source.lastIndexOf(File.separatorChar) + 1);
        String sourcePath = source.substring(0, source.lastIndexOf(File.separatorChar));
        String destPath = dest.substring(0, dest.lastIndexOf(File.separatorChar));

        File folder = new File(sourcePath);
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

        if (folder.isDirectory())
            for (File file : Objects.requireNonNull(folder.listFiles())) {
                String name = file.getName();
                if (matcher.matches(file.toPath().getFileName())) {
                    copySimpleFile(sourcePath + File.separatorChar + name, destPath + File.separatorChar + name);
                }
            }
    }
}
