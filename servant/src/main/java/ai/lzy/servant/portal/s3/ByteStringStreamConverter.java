package ai.lzy.servant.portal.s3;

import com.google.protobuf.ByteString;
import ru.yandex.qe.s3.repository.BiDirectS3Converter;

import javax.annotation.WillClose;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ByteStringStreamConverter implements BiDirectS3Converter<Stream<ByteString>> {
    @Override
    @WillClose
    public void toStream(Stream<ByteString> value, OutputStream outputStream) {
        value.forEach(chunk -> {
            try {
                chunk.writeTo(outputStream);
            } catch (IOException e) {
                // intentionally blank
            }
        });
    }

    @Override
    @WillClose
    public Stream<ByteString> fromStream(InputStream inputStream) throws IOException {
        BlockingQueue<ByteString> queue = new ArrayBlockingQueue<>(1000);
        final byte[] buffer = new byte[4096];
        try (final InputStream stream = inputStream) {
            int len = 0;
            while (len != -1) {
                final ByteString chunk = ByteString.copyFrom(buffer, 0, len);
                //noinspection StatementWithEmptyBody,CheckStyle
                while (!queue.offer(chunk, 1, TimeUnit.SECONDS)) {
                }
                len = stream.read(buffer);
            }
            //noinspection StatementWithEmptyBody,CheckStyle
            while (!queue.offer(ByteString.EMPTY, 1, TimeUnit.SECONDS)) {
            }
        } catch (InterruptedException e) {
            return null;
        }
        final Iterator<ByteString> chunkIterator = new Iterator<>() {
            ByteString chunk = null;

            @Override
            public boolean hasNext() {
                try {
                    while (chunk == null) {
                        chunk = queue.poll(1, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                return chunk != ByteString.EMPTY;
            }

            @Override
            public ByteString next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final ByteString chunk = this.chunk;
                this.chunk = null;
                return chunk;
            }
        };

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(chunkIterator, Spliterator.NONNULL),
                false
        );
    }
}
