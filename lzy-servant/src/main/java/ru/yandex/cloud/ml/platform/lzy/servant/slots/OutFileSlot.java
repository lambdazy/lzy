package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.FileContents;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class OutFileSlot extends LzySlotBase implements LzyFileSlot, LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(OutFileSlot.class);
    private final Path storage;
    private final String tid;
    private boolean ready;
    private final List<Runnable> closeActions = new ArrayList<>();

    protected OutFileSlot(String tid, Slot definition, Path storage) {
        super(definition);
        this.tid = tid;
        this.storage = storage;
        ready = true;
    }

    public OutFileSlot(String tid, Slot definition) throws IOException {
        super(definition);
        this.tid = tid;
        this.storage = Files.createTempFile("lzy", "file-slot");
        ready = false;
    }

    @Override
    public Path location() {
        return Path.of(name());
    }

    @Override
    public long size() {
        try {
            return Files.size(storage);
        } catch (IOException e) {
            LOG.warn("Unable to get a storage file size", e);
            return 0;
        }
    }

    @Override
    public long ctime() {
        try {
            return ((FileTime)Files.getAttribute(storage, "unix:creationTime")).toMillis();
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long mtime() {
        try {
            return ((FileTime)Files.getAttribute(storage, "unix:lastModifiedTime")).toMillis();
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long atime() {
        try {
            return ((FileTime)Files.getAttribute(storage, "unix:lastAccessTime")).toMillis();
        } catch (IOException e) {
            LOG.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    public int mtype() {
        return FileStat.S_IFREG;
    }

    @Override
    public void remove() throws IOException {
        Files.delete(storage);
    }

    @Override
    public FileContents open(FuseFileInfo fi) throws IOException {
        final LocalFileContents localFileContents = new LocalFileContents(storage,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ
        );
        localFileContents.onClose(written -> {
            if (written) {
                synchronized (OutFileSlot.this) {
                    LOG.info("Content to slot " + OutFileSlot.this + " was written; READY=true");
                    ready = true;
                    state(Operations.SlotStatus.State.OPEN);
                    OutFileSlot.this.notifyAll();
                }
            }
        });
        return localFileContents;
    }

    public void flush() {
        try {
            Files.writeString(storage, "empty", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Operations.SlotStatus status() {
        final Operations.SlotStatus.Builder builder = Operations.SlotStatus.newBuilder()
            .setState(state())
            .setDeclaration(gRPCConverter.to(definition()));
        if (tid != null) {
            builder.setTaskId(tid);
        }
        return builder.build();
    }

    @Override
    public synchronized Stream<ByteString> readFromPosition(long offset) throws IOException {
        LOG.info("OutFileSlot.readFromPosition for slot " + this.definition().name());
        while (!ready) {
            try {
                this.wait();
            }
            catch (InterruptedException ignore) {}
        }
        LOG.info("Slot {} is ready", name());
        final FileChannel channel = FileChannel.open(storage);
        channel.position(offset);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private final ByteBuffer bb = ByteBuffer.allocate(4096);
            @Override
            public boolean hasNext() {
                if (state() != Operations.SlotStatus.State.OPEN) {
                    LOG.info("Slot {} hasNext is not open", name());
                    return false;
                }
                try {
                    bb.clear();
                    int read = channel.read(bb);
                    LOG.info("Slot {} hasNext read {}", name(), read);
                    return read >= 0;
                }
                catch (IOException e) {
                    LOG.warn("Unable to read line from reader", e);
                    return false;
                }
            }

            @Override
            public ByteString next() {
                bb.flip();
                LOG.info("Send from slot {} data {}", name(), bb.toString());
                return ByteString.copyFrom(bb);
            }
        }, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.DISTINCT), false);
    }

    public void destroy() {
        super.destroy();
        ForkJoinPool.commonPool().execute(() -> {
            Thread.yield();
            closeActions.forEach(Runnable::run);
        });
    }

    @Override
    public String toString() {
        return "OutFileSlot:" + definition().name() + "->" + storage.toString();
    }
}
