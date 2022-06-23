package ai.lzy.fs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.fs.fs.LzyLinuxFsManagerImpl;
import ai.lzy.fs.fs.LzyMacosFsManagerImpl;
import ai.lzy.model.Slot;
import ai.lzy.model.data.DataSchema;
import ai.lzy.fs.slots.InFileSlot;
import ai.lzy.priv.v2.Operations.SlotStatus.State;

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
        FileUtils.deleteDirectory(new File(LZY_MOUNT));
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
        }, tempFile);
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
        stream.flush();
        slot.state(State.OPEN);

        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}
