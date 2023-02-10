package ai.lzy.fs.slots;

import ai.lzy.fs.fs.FileContents;
import ai.lzy.fs.fs.LzyFileSlot;
import ai.lzy.fs.fs.LzyOutputSlotBase;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.slots.LSA;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jnr.constants.platform.OpenFlags;
import org.apache.logging.log4j.Logger;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.lzy.v1.common.LMS.SlotStatus.State.OPEN;
import static ai.lzy.v1.common.LMS.SlotStatus.State.PREPARING;
import static ai.lzy.v1.common.LMS.SlotStatus.State.UNBOUND;

public class OutFileSlot extends LzyOutputSlotBase implements LzyFileSlot {
    public static final int PAGE_SIZE = 4096;
    private final Path storage;
    private final CompletableFuture<Supplier<FileChannel>> channelSupplier = new CompletableFuture<>();

    public OutFileSlot(SlotInstance instance) throws IOException {
        super(instance);
        this.storage = Files.createTempFile("lzy", "file-slot");
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
            log.warn("Unable to get a storage file size", e);
            return 0;
        }
    }

    @Override
    public long ctime() {
        try {
            return ((FileTime) Files.getAttribute(storage, "unix:creationTime")).toMillis();
        } catch (IOException e) {
            log.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long mtime() {
        try {
            return ((FileTime) Files.getAttribute(storage, "unix:lastModifiedTime")).toMillis();
        } catch (IOException e) {
            log.warn("Unable to get file creation time", e);
            return 0L;
        }
    }

    @Override
    public long atime() {
        try {
            return ((FileTime) Files.getAttribute(storage, "unix:lastAccessTime")).toMillis();
        } catch (IOException e) {
            log.warn("Unable to get file creation time", e);
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
        final int flags = fi.flags.intValue();
        final boolean hasWrite = (flags & (OpenFlags.O_WRONLY.intValue() | OpenFlags.O_RDWR.intValue())) != 0;
        final LocalFileContents localFileContents;
        if (hasWrite) {
            if (state() != UNBOUND) {
                throw new RuntimeException("The storage file must we written once");
            }
            localFileContents = new LocalFileContents(storage,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ
            );
            state(PREPARING);
        } else {
            localFileContents = new LocalFileContents(storage, StandardOpenOption.READ);
        }
        localFileContents.track(new FileContents.ContentsTracker() {
            final AtomicBoolean hasWrite = new AtomicBoolean(false);
            @Override
            public void onWrite(long offset, ByteBuffer chunk) {
                hasWrite.set(true);
            }

            @Override
            public void onClose() {
                if (hasWrite.get()) {
                    final byte[] page = new byte[PAGE_SIZE];
                    try (final InputStream is = new FileInputStream(storage.toFile())) {
                        int read;
                        while ((read = is.read(page)) >= 0) {
                            onChunk(ByteString.copyFrom(page, 0, read));
                        }
                    } catch (IOException e) {
                        log.warn("Unable to read contents of the slot: " + definition(), e);
                    }
                    channelSupplier.complete(() -> { // channels are now ready to read
                        try {
                            return FileChannel.open(storage);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    state(OPEN);
                }
            }
        });
        return localFileContents;
    }

    @Override
    public LMS.SlotStatus status() {
        return LMS.SlotStatus.newBuilder()
            .setState(state())
            .setDeclaration(ProtoConverter.toProto(definition()))
            .setTaskId(taskId())
            .build();
    }


    @Override
    public void readFromPosition(long offset, StreamObserver<LSA.SlotDataChunk> responseObserver) {
        log.info("OutFileSlot.readFromPosition for slot " + this.definition().name() + ", current state " + state());
        final FileChannel channel;
        waitForState(OPEN);

        if (state() != OPEN) {
            responseObserver.onError(
                Status.INTERNAL.withDescription("Slot is not open, cannot read").asException()
            );
            return;
        }

        try {
            channel = channelSupplier.get().get();
        } catch (InterruptedException | ExecutionException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription("Slot is not open, cannot read").asException()
            );
            return;
        }

        log.info("Slot {} is ready", name());

        readFileChannel(name(), offset, channel, completedReads::getAndIncrement, responseObserver, log);
    }

    public static void readFileChannel(String filename, long offset, FileChannel channel, Runnable onComplete,
                                       StreamObserver<LSA.SlotDataChunk> responseObserver, Logger log)
    {
        try {
            channel.position(offset);
        } catch (IOException e) {
            log.error("Error while reading from file channel: ", e);
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }

        final ByteBuffer bb = ByteBuffer.allocate(PAGE_SIZE);

        int read = 0;

        while (read >= 0) {
            bb.clear();

            try {
                read = channel.read(bb);
                log.info("Slot {} hasNext read {}", filename, read);
            } catch (IOException e) {
                log.error("Error while reading from file channel: ", e);
                responseObserver.onError(Status.INTERNAL.asException());
                return;
            }

            bb.flip();
            responseObserver.onNext(
                LSA.SlotDataChunk.newBuilder()
                    .setChunk(ByteString.copyFrom(bb))
                    .build()
            );
        }

        responseObserver.onNext(
            LSA.SlotDataChunk.newBuilder()
                .setControl(LSA.SlotDataChunk.Control.EOS)
                .build()
        );

        responseObserver.onCompleted();

        onComplete.run();
    }

    public static Stream<ByteString> readFileChannel(String filename, long offset, FileChannel channel,
                                                     BooleanSupplier readyFn, Logger log) throws IOException
    {
        channel.position(offset);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private final ByteBuffer bb = ByteBuffer.allocate(PAGE_SIZE);

            @Override
            public boolean hasNext() {
                if (!readyFn.getAsBoolean()) {
                    log.info("Slot {} hasNext is not open", filename);
                    return false;
                }
                try {
                    bb.clear();
                    int read = channel.read(bb);
                    log.info("Slot {} hasNext read {}", filename, read);
                    return read >= 0;
                } catch (IOException e) {
                    log.warn("Unable to read line from reader", e);
                    return false;
                }
            }

            @Override
            public ByteString next() {
                bb.flip();
                return ByteString.copyFrom(bb);
            }
        }, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.DISTINCT), false);
    }

    @Override
    public String toString() {
        return "OutFileSlot:" + definition().name() + "->" + storage.toString();
    }
}
