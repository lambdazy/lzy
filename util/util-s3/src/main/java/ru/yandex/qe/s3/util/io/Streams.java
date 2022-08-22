package ru.yandex.qe.s3.util.io;

import com.amazonaws.util.LengthCheckInputStream;
import com.gc.iotools.stream.is.inspection.StatsInputStream;
import com.gc.iotools.stream.os.inspection.StatsOutputStream;
import com.gc.iotools.stream.utils.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * User: terry Date: 06.09.13 Time: 22:51
 */
public class Streams {

    private static final Logger LOG = LoggerFactory.getLogger(Streams.class);

    public static StatsInputStream autoLogStatStream(final InputStream inputStream, final String toAppendToLog) {
        return new StatsInputStream(inputStream) {
            boolean closed = false;

            @Override
            public void close() throws IOException {
                super.close();
                if (closed) {
                    return;
                }
                closed = true;
                final long size = getSize();
            }
        };
    }

    public static StatsOutputStream autoLogStatStream(final OutputStream outputStream, final String toAppendToLog) {
        return new StatsOutputStream(outputStream) {
            boolean closed = false;

            @Override
            public void close() throws IOException {
                super.close();
                if (closed) {
                    return;
                }
                closed = true;
                final long size = getSize();
            }
        };
    }

    private static String formatSize(long size) {
        final String result = StreamUtils.getRateString(size, 1000);
        return result.substring(0, result.length() - 4);
    }

    public static InputStream checkLength(final InputStream inputStream, long expectedBytesCount) {
        return new LengthCheckInputStream(inputStream, expectedBytesCount,
            LengthCheckInputStream.INCLUDE_SKIPPED_BYTES);
    }

    public static InputStream checkLength(final InputStream inputStream, long expectedBytesCount,
        boolean includeSkipped) {
        return new LengthCheckInputStream(inputStream, expectedBytesCount, includeSkipped);
    }
}
