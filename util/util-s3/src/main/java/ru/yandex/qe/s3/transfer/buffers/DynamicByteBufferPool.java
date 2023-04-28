package ru.yandex.qe.s3.transfer.buffers;

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Established by terry on 14.07.15.
 */
public class DynamicByteBufferPool extends GenericObjectPool<ByteBuffer> implements ByteBufferPool {

    private final ByteBufferSizeType byteBufferSizeType;

    public DynamicByteBufferPool(ByteBufferSizeType byteBufferSizeType, int poolSize) {
        this(byteBufferSizeType, poolSize, poolSize / 2);
    }

    public DynamicByteBufferPool(ByteBufferSizeType byteBufferSizeType, int poolSize, int idleSize) {
        super(new BasePooledObjectFactory<ByteBuffer>() {
            @Override
            public ByteBuffer create() throws Exception {
                return ByteBuffer.allocate(byteBufferSizeType.getSizeInBytes());
            }

            @Override
            public PooledObject<ByteBuffer> wrap(ByteBuffer buffer) {
                return new DefaultPooledObject<>(buffer);
            }
        });
        this.byteBufferSizeType = byteBufferSizeType;
        setMaxTotal(poolSize);
        setMaxIdle(idleSize);
        setBlockWhenExhausted(true);
    }

    @Override
    public void returnObject(@Nonnull ByteBuffer buffer) {
        buffer.clear();
        super.returnObject(buffer);
    }

    @Override
    public int bufferSizeBytes() {
        return byteBufferSizeType.getSizeInBytes();
    }

    @Nonnull
    @Override
    public Map<String, Supplier<?>> metrics() {
        final ImmutableMap.Builder<String, Supplier<?>> metrics = ImmutableMap.builder();
        metrics.put("borrow.count", this::getBorrowedCount);
        metrics.put("borrow.wait.mean", this::getMeanBorrowWaitTimeMillis);
        metrics.put("returned.count", this::getReturnedCount);
        metrics.put("created.count", this::getCreatedCount);
        metrics.put("destroyed.count", this::getDestroyedCount);
        metrics.put("max.count", this::getMaxTotal);
        metrics.put("idle.count", this::getNumIdle);
        metrics.put("active.count", this::getNumActive);

        return metrics.build();
    }
}
