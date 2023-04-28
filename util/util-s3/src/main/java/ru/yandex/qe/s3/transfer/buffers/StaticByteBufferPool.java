package ru.yandex.qe.s3.transfer.buffers;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qe.s3.util.HumanReadable;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Byte buffer pool that pre-allocates all the buffers.<br> Useful for debugging {@code OutOfMemoryError}s.
 *
 * @author entropia
 */
public final class StaticByteBufferPool implements ByteBufferPool {

    private static final Logger LOG = LoggerFactory.getLogger(StaticByteBufferPool.class);

    private final Object poolLock = new Object();

    private final int bufSizeBytes;

    private final Set<ObjWrapper<ByteBuffer>> borrowed;
    private final BlockingQueue<ByteBuffer> available;

    public StaticByteBufferPool(@Nonnull ByteBufferSizeType sizeType, int count) {
        Preconditions.checkArgument(count > 0, "byte buffer count must be > 0");
        this.borrowed = new LinkedHashSet<>();
        this.available = new LinkedBlockingDeque<>(count);
        this.bufSizeBytes = sizeType.getSizeInBytes();
        for (int i = 0; i < count; i++) {
            final ByteBuffer buf = ByteBuffer.allocate(bufSizeBytes);
            available.add(buf);
        }
        LOG.debug("pre-allocated {} byte buffer(s) totaling {}", count,
            HumanReadable.fileSize(((long) count) * bufSizeBytes));
    }

    @Nonnull
    @Override
    public ByteBuffer borrowObject(long timeoutMs) {
        synchronized (poolLock) {
            final ByteBuffer buf = doPoll(timeoutMs);
            if (buf == null) {
                throw new NoSuchElementException("Byte buffer pool is exhausted");
            }

            final ObjWrapper<ByteBuffer> wrapper = new ObjWrapper<>(buf);
            borrowed.add(wrapper);
            LOG.debug("borrowed byte buffer: {}", wrapper);

            return buf;
        }
    }

    @Nullable
    private ByteBuffer doPoll(long timeoutMs) {
        try {
            return available.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debug("got interrupted while waiting for byte buffer from pool", e);
            return null;
        }
    }

    @Override
    public void returnObject(@Nonnull ByteBuffer buf) {
        synchronized (poolLock) {
            final ObjWrapper<ByteBuffer> wrapper = new ObjWrapper<>(buf);
            Preconditions.checkState(borrowed.remove(wrapper), "returning buffer %s that was not borrowed", wrapper);
            Preconditions.checkState(available.offer(buf), "could not make returned buffer available");

            buf.clear();

            LOG.debug("returned borrowed byte buffer: {}", wrapper);
        }
    }

    @Override
    public int bufferSizeBytes() {
        return bufSizeBytes;
    }

    private static final class ObjWrapper<T> {

        private final T obj;

        private ObjWrapper(@Nonnull T obj) {
            this.obj = Objects.requireNonNull(obj, "obj");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ObjWrapper)) {
                return false;
            }

            ObjWrapper<?> that = (ObjWrapper<?>) o;
            return obj == that.obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(obj);
        }

        @Override
        public String toString() {
            return "@" + hashCode();
        }
    }
}
