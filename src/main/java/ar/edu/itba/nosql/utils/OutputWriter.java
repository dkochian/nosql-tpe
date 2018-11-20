package ar.edu.itba.nosql.utils;

import ar.edu.itba.nosql.entities.Trajectory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;

public class OutputWriter {

    public OutputWriter() {
        final File file = new File("output");
        if (!file.exists())
            if (!file.mkdirs())
                throw new RuntimeException("Couldn't create the output directory.");
    }

    public void write(Queue<Trajectory> trajectoryPrunned, String tableName) throws IOException {
        final String path = "output" + "/" + "prunned_" + tableName + ".tsv";
        try (final PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))) {
            for (Trajectory t : trajectoryPrunned)
                printWriter
                        .append(String.valueOf(t.getUserId()))
                        .append("\t")
                        .append(String.valueOf(t.getVenue().getId()))
                        .append("\t")
                        .append(String.valueOf(t.getDate()))
                        .append("\t")
                        .append(String.valueOf(t.getTpos()))
                        .append("\r\n");

            printWriter.flush();
        }
    }

    public void remove() {
        final Path p = Paths.get("output" + "/" + "prunned.tsv");

        if (Files.exists(p)) {
            try {
                Files.delete(p);
            } catch (IOException e) {
            }
        }
    }
}
