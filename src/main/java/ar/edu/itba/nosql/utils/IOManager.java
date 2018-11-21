package ar.edu.itba.nosql.utils;

import ar.edu.itba.nosql.entities.Configuration;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class IOManager {

    private static final Logger logger = LoggerFactory.getLogger(IOManager.class);
    private static final String FILENAME = "config.json";

    public static Configuration getConfiguration() {
        Configuration configuration;
        try {
            logger.debug("Loading configuration");
            configuration = read(FILENAME, Configuration.class);
        } catch (IOException e) {
            try {
                logger.debug("Writing configuration");
                write(FILENAME, new Configuration());
            } catch (IOException e1) {
                logger.error(e.getMessage());
            }
            return new Configuration();
        }

        return configuration;
    }

    public static <T> T read(final String filename, Class<T> clazz) throws IOException {
        checkAndCreateFolder(filename);
        try (final Reader reader = new BufferedReader(new FileReader(filename))) {
            final Gson gson = (new GsonBuilder()).create();
            return clazz.cast(gson.fromJson(reader, clazz));
        }
    }

    public static <T> void write(final String filename, final T object) throws IOException {
        checkAndCreateFolder(filename);
        try (final Writer writer = new BufferedWriter(new FileWriter(filename))) {
            final Gson gson = (new GsonBuilder()).setPrettyPrinting().create();
            gson.toJson(object, writer);
        }
    }

    private static void checkAndCreateFolder(final String filename) {
        final int index = filename.lastIndexOf('/');
        if (index != -1) {
            final String folder = filename.substring(0, index);
            final File file = new File(folder);
            if (!file.exists())
                if (!file.mkdirs())
                    throw new RuntimeException("Couldn't create the folder: " + folder);
        }
    }

}
