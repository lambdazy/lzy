package ru.yandex.qe.s3.transfer;

import org.apache.commons.io.IOUtils;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

import java.io.*;

/**
 * Established by terry on 16.07.15.
 */
public class DownloadConsumers {

    public static ThrowingConsumer<MetaAndStream> sinkWillClose(OutputStream outputStream) {
        try {
            return sink(outputStream);
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) { //ignored
            }
        }
    }

    public static ThrowingConsumer<MetaAndStream> sink(OutputStream outputStream) {
        return metaAndStream -> {
            try (InputStream inputStream = metaAndStream.getInputStream()) {
                IOUtils.copyLarge(inputStream, outputStream);
            }
        };
    }

    public static ThrowingConsumer<MetaAndStream> sink(File file) {
        return metaAndStream -> {
            try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file))) {
                try (InputStream inputStream = metaAndStream.getInputStream()) {
                    IOUtils.copyLarge(inputStream, outputStream);
                }
            }
        };
    }
}
