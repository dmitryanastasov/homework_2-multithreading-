package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        final File resultFile = new File(resultFileName);
        if (!resultFile.exists()) {
            try {
                resultFile.createNewFile();
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
        final Exchanger<Pair<String, Integer>> exchanger = new Exchanger<>();
        final AtomicInteger rawCounter = new AtomicInteger(1);
        // Запускаем FileWriter в отдельном потоке
        final Thread fileWriterThread = new Thread(new FileWriter(exchanger, resultFile, rawCounter));
        final LineProcessor<Integer> counterProcessor = new LineCounterProcessor();
        fileWriterThread.setDaemon(true);
        fileWriterThread.start();
        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            int countRaw = 1;
            while (scanner.hasNext()) {
                int finalCountRaw = countRaw;
                String nextLine = scanner.nextLine();
                executorService.execute(new Runnable() {
                    int myCount = finalCountRaw;
                    @Override
                    public void run() {
                        Pair<String, Integer> process = counterProcessor.process(nextLine);
                        while (myCount != rawCounter.get()) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                return;
                            }
                        }
                        try {
                            exchanger.exchange(process);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                countRaw++;
            }
        } catch (IOException exception) {
            logger.error("", exception);
        }
        try {
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();
        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}