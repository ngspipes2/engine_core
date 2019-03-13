package pt.isel.ngspipes.engine_core.utils;

import java.io.*;
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
        int separatorIdx = pattern.indexOf(File.separatorChar);
        String currPattern;
        if (separatorIdx != -1) {
            directoryPath = directoryPath + File.separatorChar + pattern.substring(0, separatorIdx);
            currPattern = pattern.substring(separatorIdx + 1);
        } else
            currPattern = pattern;
        File dir = new File(directoryPath);
        List<String> names = new LinkedList<>();
        File [] files = dir.listFiles((dir1, name) -> name.matches(currPattern));

        if (files != null) {
            for (File file : files)
                names.add(file.getName());
        }

        return names;
    }

    public static void findFiles(String basePath, String pattern) throws IOException {
        List<String> names = getFileNamesByPattern(basePath, pattern);
        if (names.isEmpty())
            throw new IOException("Match not found for " + pattern + " on directory " + basePath);
    }


    private static void copySimpleFile(String source, String dest) throws IOException {
        Path sourcePath = Paths.get(source);
        Path destPath = Paths.get(dest);
        File destFile = new File(destPath.toString());
        if (!destFile.getParentFile().exists())
            destFile.getParentFile().mkdirs();
        Files.copy(sourcePath, destPath);
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

    public static void writeFile(String path, String content) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }
        try(PrintWriter writer = new PrintWriter(path, "UTF-8")){
            writer.println(content);
        }
    }
}
