package ru.yandex.qe.s3.transfer;

import static org.testng.Assert.*;

import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.springframework.test.context.ActiveProfiles;
import org.testng.Assert;
import org.testng.annotations.Test;
import ru.yandex.qe.s3.transfer.buffers.DynamicByteBufferPool;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferSizeType;
import ru.yandex.qe.s3.transfer.buffers.StaticByteBufferPool;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * Established by terry
 * on 30.07.15.
 */
@ActiveProfiles("testing")
public class ByteBufferPoolTest {

    @Test
    public void dyn_check_buffer_clear_on_return() throws Exception {
        final DynamicByteBufferPool bufferPool = new DynamicByteBufferPool(ByteBufferSizeType._8_MB, 1, 1);
        final ByteBuffer byteBuffer = bufferPool.borrowObject();
        byteBuffer.limit(10).position(2);
        bufferPool.returnObject(byteBuffer);
        final ByteBuffer newByteBuffer = bufferPool.borrowObject();
        Assert.assertSame(newByteBuffer, byteBuffer);
        MatcherAssert.assertThat(newByteBuffer.limit(), Is.is(byteBuffer.capacity()));
        MatcherAssert.assertThat(newByteBuffer.position(), Is.is(0));
    }

    @Test
    public void dyn_check_borrow_blocked_if_all_borrowed() throws Exception {
        final DynamicByteBufferPool bufferPool = new DynamicByteBufferPool(ByteBufferSizeType._8_MB, 1, 1);
        bufferPool.borrowObject();
        try {
            bufferPool.borrowObject(100);
            fail();
        }catch (NoSuchElementException ex) {
            assertTrue(ex.getMessage().startsWith("Timeout waiting for idle object"));
        }
    }

    @Test
    public void static_check_buffer_clear_on_return() throws Exception {
        final StaticByteBufferPool bufferPool = new StaticByteBufferPool(ByteBufferSizeType._8_MB, 1);
        final ByteBuffer byteBuffer = bufferPool.borrowObject(100);
        byteBuffer.limit(10).position(2);
        bufferPool.returnObject(byteBuffer);
        final ByteBuffer newByteBuffer = bufferPool.borrowObject(100);
        Assert.assertSame(newByteBuffer, byteBuffer);
        MatcherAssert.assertThat(newByteBuffer.limit(), Is.is(byteBuffer.capacity()));
        MatcherAssert.assertThat(newByteBuffer.position(), Is.is(0));
    }

    @Test
    public void static_check_borrow_blocked_if_all_borrowed() throws Exception {
        final StaticByteBufferPool bufferPool = new StaticByteBufferPool(ByteBufferSizeType._8_MB, 2);
        final ByteBuffer borrowed = bufferPool.borrowObject(100);
        assertNotNull(borrowed, "borrowed first pool successfully");
        final ByteBuffer borrowed2 = bufferPool.borrowObject(100);
        assertNotNull(borrowed2, "borrowed second pool successfully");
        assertNotSame(borrowed, borrowed2, "borrow returns different objects if nothing was returned");
        try {
            bufferPool.borrowObject(100);
            fail("should have caught NoSuchElementException");
        } catch (NoSuchElementException expected) {
            // do nothing
        }
    }
}
