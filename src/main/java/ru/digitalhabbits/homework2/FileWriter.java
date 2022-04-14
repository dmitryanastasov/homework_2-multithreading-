package ru.digitalhabbits.homework2;

import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private String result;
    private Exchanger<List<String>> exchanger;

    public FileWriter(String result, Exchanger<List<String>> exchanger) {
        this.exchanger = exchanger;
        this.result = result;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        final File file = new File(result);
        try (BufferedWriter outputWriter = new BufferedWriter(new java.io.FileWriter(file))) {
            List<String> resultList = new ArrayList<>();
            while (!currentThread().isInterrupted()) {
                try {
                    resultList = exchanger.exchange(resultList);
                } catch (InterruptedException e) {
                    break;
                }
                for (String line : resultList) {
                    outputWriter.write(line);
                    outputWriter.newLine();
                }
            }
        } catch (IOException exception) {
            logger.error("Exception", exception);
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
