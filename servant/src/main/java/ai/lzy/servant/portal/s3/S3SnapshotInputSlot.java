package ai.lzy.servant.portal.s3;

import ai.lzy.fs.slots.LzyInputSlotBase;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.SlotInstance;
import ai.lzy.v1.Operations;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadState;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class S3SnapshotInputSlot extends LzyInputSlotBase {

    private static final Logger LOG = LogManager.getLogger(S3SnapshotInputSlot.class);
    private static final ThreadGroup READER_TG = new ThreadGroup("input-slot-readers");

    private final OutputStream out;
    private final Pipe pipe;
    private final ListenableFuture<UploadState> future;

    public S3SnapshotInputSlot(SlotInstance instance, StorageClient storageClient, String bucket) {
        super(instance);
        try {
            this.pipe = Pipe.open();
        } catch (IOException e) {
            throw new RuntimeException("Cannot create pipe ", e);
        }
        this.out = Channels.newOutputStream(pipe.sink());
        future = storageClient.transmitter().upload(new UploadRequestBuilder()
                .bucket(bucket)
                .key(generateKey())
                .metadata(Metadata.empty())
                .stream(() -> Channels.newInputStream(pipe.source()))
                .build()
        );
    }

    private String generateKey() {
        return "slot_" + instance().name();
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> inputData) {
        super.connect(slotUri, inputData);
        LOG.info("Attempt to connect from slot {} to {}", instance().name(), slotUri);

        var dataReader = new Thread(READER_TG, this::readAll,
                "reader-FROM-%s-TO-%s".formatted(slotUri, definition().name()));
        dataReader.start();

        onState(Operations.SlotStatus.State.CLOSED, dataReader::interrupt);
    }

    @Override
    public void onChunk(ByteString bytes) throws IOException {
        super.onChunk(bytes);
        LOG.info("S3SnapshotInputSlot::onChunk invoked with slot " + instance().name());
        out.write(bytes.toByteArray());
    }

    @Override
    public void close() {
        super.close();
        try {
            out.close();
        } catch (IOException e) {
            LOG.warn("Error while closing output stream {} to s3-storage: {}", out, e.getMessage());
        }
    }

    @Override
    public void onFinish() {
        LOG.info("S3SnapshotInputSlot::onFinish invoked with slot " + instance().name());
        try {
            out.close();
            future.get();
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
