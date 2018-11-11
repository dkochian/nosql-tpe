package ar.edu.itba.nosql.utils;

import ar.edu.itba.nosql.entities.Trajectory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;

public class OutputWriter {

    public void write(Queue<Trajectory> trajectoryPrunned) throws IOException {
        final String path = "output" + "/" + "prunned.tsv";
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
