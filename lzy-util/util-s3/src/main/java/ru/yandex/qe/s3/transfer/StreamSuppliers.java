package ru.yandex.qe.s3.transfer;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

/**
 * Established by terry on 13.07.15. !!Upload implementation will invoke supplier only once and close it in finally
 * block!!
 */
public class StreamSuppliers {

    public static ThrowingSupplier<InputStream> of(InputStream inputStream) {
        return new StreamSupplier(inputStream);
    }

    public static ThrowingSupplier<InputStream> of(Supplier<InputStream> supplier) {
        return supplier::get;
    }

    public static ThrowingSupplier<InputStream> lazy(File file) {
        return new FileStreamSupplier(file);
    }

    public static ThrowingSupplier<InputStream> lazy(ThrowingConsumer<OutputStream> consumer,
        ExecutorService executorService, int pipedChunkSize) {
        return new OutputConsumerStreamSupplier(consumer, executorService, pipedChunkSize);
    }

    private static class StreamSupplier implements ThrowingSupplier<InputStream> {

        private final InputStream inputStream;

        public StreamSupplier(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public InputStream getThrows() throws Exception {
            return inputStream;
        }
    }

    private static class FileStreamSupplier implements ThrowingSupplier<InputStream> {

        private final File sourceFile;

        public FileStreamSupplier(File sourceFile) {
            this.sourceFile = sourceFile;
        }

        @Override
        public InputStream getThrows() throws Exception {
            return new BufferedInputStream(new FileInputStream(sourceFile));
        }
    }

    private static class OutputConsumerStreamSupplier implements ThrowingSupplier<InputStream> {

        private final ThrowingConsumer<OutputStream> consumer;
        private final ExecutorService executorService;
        private final int pipedChunkSize;


        public OutputConsumerStreamSupplier(ThrowingConsumer<OutputStream> consumer, ExecutorService executorService,
            int pipedChunkSize) {
            this.consumer = consumer;
            this.executorService = executorService;
            this.pipedChunkSize = pipedChunkSize;
        }

        @Override
        public InputStream getThrows() throws Exception {
            return new InputStreamFromOutputStream<Void>(true, executorService, pipedChunkSize) {
                @Override
                protected Void produce(OutputStream sink) throws Exception {
                    consumer.accept(sink);
                    return null;
                }
            };
        }
    }
}
