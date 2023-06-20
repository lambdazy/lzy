package ai.lzy.fs;

import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyLinuxFsManagerImpl;
import ai.lzy.fs.fs.LzyMacosFsManagerImpl;
import ai.lzy.fs.slots.InFileSlot;
import ai.lzy.model.DataScheme;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS.SlotStatus.State;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LzyFSTest {
    private static final String LZY_MOUNT = "/tmp/lzy-" + UUID.randomUUID();
    private LzyFSManager lzyFS;

    @Before
    public void setUp() {
        if (SystemUtils.IS_OS_MAC) {
            lzyFS = new LzyMacosFsManagerImpl();
        } else if (SystemUtils.IS_OS_LINUX) {
            lzyFS = new LzyLinuxFsManagerImpl();
        } else {
            Assert.fail("Unsupported OS");
        }
        lzyFS.mount(Path.of(LZY_MOUNT));
    }

    @After
    public void tearDown() {
        lzyFS.umount();
        try {
            FileUtils.deleteDirectory(new File(LZY_MOUNT));
        } catch (Exception e) {
            // ignored
        }
    }

    @Test
    public void testFuseConfiguration() {
        // fusermount: option allow_other only allowed if 'user_allow_other' is set in /etc/fuse.conf
        System.out.println("Ok");
    }

    private record SlotDesc(
        InFileSlot slot,
        Path file
    ) {}

    private SlotDesc prepareSlot(boolean allowMultipleRead) throws Exception {
        final String slotPath = "/test_in_slot";
        final Path tempFile = Files.createTempFile("lzy", "test-file-slot-" + UUID.randomUUID());
        final InFileSlot slot = new InFileSlot(
            new SlotInstance(
                new Slot() {
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
                    public DataScheme contentType() {
                        return DataScheme.PLAIN;
                    }
                },
                "taskId",
                "channelId",
                new URI("slot", "host", "/path", null)
            ), tempFile, allowMultipleRead);
        lzyFS.addSlot(slot);

        return new SlotDesc(slot, tempFile);
    }

    @Test
    public void testSingleRead() throws Exception {
        var slot = prepareSlot(false);

        var stream = Files.newOutputStream(slot.file());

        stream.write("Some text".getBytes());
        stream.close();
        slot.slot().state(State.OPEN);

        var content = new String(Files.readAllBytes(Path.of(LZY_MOUNT, slot.slot().name())));
        Assert.assertEquals("Some text", content);

        while (slot.slot().state() != State.SUSPENDED) {
            LockSupport.parkNanos(50);
        }

        content = new String(Files.readAllBytes(Path.of(LZY_MOUNT, slot.slot().name())));
        Assert.assertEquals("", content);
    }

    @Test
    public void testMultipleRead() throws Exception {
        var slot = prepareSlot(true);

        var stream = Files.newOutputStream(slot.file());

        stream.write("Some text".getBytes());
        stream.close();
        slot.slot().state(State.OPEN);

        for (int i = 0; i < 10; ++i) {
            var content = new String(Files.readAllBytes(Path.of(LZY_MOUNT, slot.slot().name())));
            Assert.assertEquals("attempt #" + i, "Some text", content);
        }
    }

    @Test
    public void testWaitForSlot() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        var slot = prepareSlot(false);

        var stream = Files.newOutputStream(slot.file());
        stream.write("Some".getBytes());

        final var result = new String[] {""};
        ForkJoinPool.commonPool().execute(() -> {
            byte[] buffer = new byte[4096];
            try (var is = Files.newInputStream(Path.of(LZY_MOUNT, slot.slot().name()), StandardOpenOption.READ)) {
                int read;
                while ((read = is.read(buffer)) >= 0) {
                    result[0] += new String(buffer, 0, read);
                    System.out.write(buffer, 0, read);
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        });

        stream.write(" text\n".getBytes());
        stream.flush();
        slot.slot().state(State.OPEN);

        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        Assert.assertEquals("Some text\n", result[0]);

        stream.close();
    }
}
