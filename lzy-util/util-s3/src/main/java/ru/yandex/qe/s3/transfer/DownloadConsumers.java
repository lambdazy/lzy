package ru.yandex.qe.s3.transfer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

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
