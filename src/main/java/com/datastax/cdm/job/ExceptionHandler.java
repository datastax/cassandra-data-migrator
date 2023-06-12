package com.datastax.cdm.job;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class ExceptionHandler {

    private static final String NEW_LINE = System.lineSeparator();

    private static void appendToFile(Path path, String content)
            throws IOException {
        // if file not exists, create and write to it
        // otherwise append to the end of the file
        Files.write(path, content.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    public static void FileAppend(String dir, String fileName, String content) throws IOException {

        //create directory if not already existing
        Files.createDirectories(Paths.get(dir));
        Path path = Paths.get(dir + "/" + fileName);
        appendToFile(path, content + NEW_LINE);

    }

}
