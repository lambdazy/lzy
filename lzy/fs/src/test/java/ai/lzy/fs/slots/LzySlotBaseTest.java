package ai.lzy.fs.slots;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.DataScheme;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS.SlotStatus.State;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LzySlotBaseTest {

    // FIXME(d-kruchinin): does it really necessary?
    @Ignore
    @Test
    public void obsoleteActions() throws InterruptedException {
        var lzySlot = new LzySlotBase(getSlotInstance()) {};

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        lzySlot.onState(State.OPEN, counter::incrementAndGet); // ok
        lzySlot.onState(State.OPEN, counter::incrementAndGet); // ok
        lzySlot.onState(State.OPEN, lzySlot::suspend);
        lzySlot.onState(State.OPEN, counter::incrementAndGet); // should not happen
        lzySlot.onState(State.SUSPENDED, latch::countDown);

        lzySlot.state(State.OPEN);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(State.SUSPENDED, lzySlot.state());
        assertEquals(2, counter.get());
    }

    @Test
    public void actionsOrder() throws InterruptedException {
        var lzySlot = new LzySlotBase(getSlotInstance()) {};

        final AtomicInteger openCounter = new AtomicInteger();
        final AtomicInteger suspendCounter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < 100; ++i) {
            final int order = i + 1;

            lzySlot.onState(State.OPEN, () -> {
                assertTrue(order <= 50);

                openCounter.incrementAndGet();
                assertEquals(order, openCounter.get());
                assertEquals(0, suspendCounter.get());
                LockSupport.parkNanos(100);

                if (order == 50) {
                    lzySlot.state(State.SUSPENDED);
                }
            });

            lzySlot.onState(State.SUSPENDED, () -> {
                suspendCounter.incrementAndGet();
                assertEquals(50, openCounter.get());
                assertEquals(order, suspendCounter.get());
                LockSupport.parkNanos(100);

                if (order == 100) {
                    latch.countDown();
                }
            });
        }

        lzySlot.state(State.OPEN);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(50, openCounter.get());
        assertEquals(100, suspendCounter.get());
    }

    @Test
    public void failedAction() throws InterruptedException {
        var lzySlot = new LzySlotBase(getSlotInstance()) {};

        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        lzySlot.onState(State.OPEN, counter::incrementAndGet); // ok
        lzySlot.onState(State.OPEN, new LzySlot.StateChangeAction() {
            @Override
            public void onError(Throwable th) {
                assertTrue(th instanceof RuntimeException);
                assertEquals("Hi there!", th.getMessage());
                lzySlot.destroy("fail");
                latch.countDown();
            }

            @Override
            public void run() {
                throw new RuntimeException("Hi there!");
            }
        });
        lzySlot.onState(State.OPEN, counter::incrementAndGet); // should not happen
        lzySlot.onState(State.OPEN, lzySlot::suspend);         // should not happen
        lzySlot.onState(State.SUSPENDED, latch::countDown);    // should not happen

        lzySlot.state(State.OPEN);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(State.DESTROYED, lzySlot.state());
        assertEquals(1, counter.get());
    }

    @Test
    @Ignore
    public void failTest() {
        var lzySlot = new LzySlotBase(getSlotInstance()) {};

        lzySlot.onState(State.OPEN, () -> {
            throw new RuntimeException("Hi there!");
        });

        lzySlot.state(State.OPEN);

        fail("should not happen");
    }

    private static SlotInstance getSlotInstance() {
        try {
            return new SlotInstance(
                    new Slot() {
                        @Override
                        public String name() {
                            return "test-slot";
                        }

                        @Override
                        public Media media() {
                            return Media.FILE;
                        }

                        @Override
                        public Direction direction() {
                            return Direction.INPUT;
                        }

                        @Override
                        public DataScheme contentType() {
                            return DataScheme.PLAIN;
                        }
                    },
                    "taskId", "channelId", new URI("slot", "host", null, null)
            );
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
