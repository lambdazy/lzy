package ai.lzy.fs;

import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyLinuxFsManagerImpl;
import ai.lzy.fs.fs.LzyMacosFsManagerImpl;
import ai.lzy.fs.slots.InFileSlot;
import ai.lzy.model.DataScheme;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS.SlotStatus.State;
import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

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
    public void tearDown() throws IOException {
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

    @Test
    public void testWaitForSlot() throws IOException, InterruptedException, URISyntaxException {
        //Arrange
        final CountDownLatch latch = new CountDownLatch(1);
        final String slotPath = "/test_in_slot";
        final Path tempFile = Files.createTempFile("lzy", "test-file-slot");
        final OutputStream stream = Files.newOutputStream(tempFile);
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
                        return null;
                    }
                },
                "taskId",
                "channelId",
                new URI("slot", "host", "/path", null)
            ), tempFile);
        lzyFS.addSlot(slot);
        stream.write(ByteString.copyFromUtf8("kek\n").toByteArray());

        //Act
        ForkJoinPool.commonPool().execute(() -> {
            byte[] buffer = new byte[4096];
            try (InputStream is = Files.newInputStream(Path.of(LZY_MOUNT, slotPath),
                StandardOpenOption.READ))
            {
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
        stream.flush();
        slot.state(State.OPEN);

        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

        stream.close();
    }
}
