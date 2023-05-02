package ru.yandex.qe.s3.transfer.buffers;

import jakarta.annotation.Nonnull;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Byte buffer pool used by {@link ru.yandex.qe.s3.transfer.loop.ProcessingLoop ProcessingLoop}s.
 * <p>
 * Default implementation in {@link DynamicByteBufferPool} is based on Apache {@code commons-pool2} and is sufficient
 * for most usages.
 *
 * @author entropia
 */
public interface ByteBufferPool {

    /**
     * Tries to borrow a {@link ByteBuffer} of size {@link #bufferSizeBytes()} from the pool, waiting at most {@code
     * timeoutMs} milliseconds for a buffer to become available.
     *
     * @param timeoutMs borrow timeout, in milliseconds
     * @return borrowed buffer
     * @throws NoSuchElementException buffer pool is exhausted
     * @throws Exception              error occurred while borrowing the buffer, e.g. could not allocate the memory
     *                                required
     */
    @Nonnull
    ByteBuffer borrowObject(long timeoutMs) throws NoSuchElementException, Exception;

    /**
     * Returns borrowed {@code ByteBuffer}.
     *
     * @param buf buffer borrowed by calling this instance's {@link #borrowObject(long)}
     * @throws IllegalStateException trying to return an object that was not borrowed or was already returned
     */
    void returnObject(@Nonnull ByteBuffer buf) throws IllegalStateException;

    /**
     * @return buffer size, in bytes
     */
    int bufferSizeBytes();

    /**
     * @return map: "metric name" => "metric value supplier"; can be empty if buffer pool has no metrics
     */
    @Nonnull
    default Map<String, Supplier<?>> metrics() {
        return Collections.emptyMap();
    }
}
