package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.InFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.DevNullSlotSnapshot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshotProvider.Cached;
import yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State;

public class LzyFSTest {
    private static final String LZY_MOUNT = "/tmp/lzy";
    private LzyFS lzyFS;

    @Before
    public void setUp() {
        lzyFS = new LzyFS();
        lzyFS.mount(Path.of("/tmp/lzy"), false, false);
    }

    @After
    public void tearDown() {
        lzyFS.umount();
    }

    @Ignore
    @Test
    public void testWaitForSlot() throws IOException, InterruptedException {
        //Arrange
        final CountDownLatch latch = new CountDownLatch(1);
        final String slotPath = "/test_in_slot";
        final Path tempFile = Files.createTempFile("lzy", "test-file-slot");
        final OutputStream stream = Files.newOutputStream(tempFile);
        final InFileSlot slot = new InFileSlot("tid", new Slot() {
            @Override
            public String name() {
                return slotPath;
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
            public DataSchema contentType() {
                return null;
            }
        }, tempFile, new Cached(DevNullSlotSnapshot::new));
        lzyFS.addSlot(slot);
        stream.write(ByteString.copyFromUtf8("kek\n").toByteArray());

        //Act
        ForkJoinPool.commonPool().execute(() -> {
            byte[] buffer = new byte[4096];
            try (InputStream is = Files.newInputStream(Path.of(LZY_MOUNT, slotPath),
                StandardOpenOption.READ)) {
                int read;
                while ((read = is.read(buffer)) >= 0) {
                    System.out.write(buffer, 0, read);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        });
        stream.write(ByteString.copyFromUtf8("kek\n").toByteArray());
        Thread.sleep(300000);
        slot.state(State.OPEN);

        //Assert
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}
