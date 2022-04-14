package ru.digitalhabbits.homework2;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);

        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);
        LineProcessor lineProcessor = new LineCounterProcessor();

        Exchanger<List<String>> exchanger = new Exchanger<>();
        Thread writerThread = new Thread(new FileWriter(resultFileName, exchanger));
        writerThread.start();

        try (final Scanner scanner = new Scanner(file, String.valueOf(defaultCharset()))) {
            while (scanner.hasNext()) {
                List<String> lineList = new ArrayList<>();
                while (lineList.size() < CHUNK_SIZE && scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    lineList.add(line);
                }
                List<Callable<String>> tasks = new ArrayList<>(lineList.size());
                for (String line : lineList) {
                    Callable<String> task = () -> lineProcessor.process(line).toString("%1$s %2$s");
                    tasks.add(task);
                }
                List<Future<String>> futureList = executorService.invokeAll(tasks);
                List<String> resultLineList = new ArrayList<>();
                for (Future<String> future : futureList) {
                    resultLineList.add(future.get());
                }
                try {
                    exchanger.exchange(resultLineList);
                } catch (InterruptedException e) {
                    logger.error("", e);
                }
            }
        } catch (IOException | InterruptedException | ExecutionException exception) {
            logger.error("", exception);
        }

        // TODO: NotImplemented: остановить поток writerThread

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
