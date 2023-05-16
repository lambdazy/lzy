package ai.lzy.portal.slots;

import ai.lzy.fs.slots.LzyInputSlotBase;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.services.PortalService;
import ai.lzy.storage.StorageClient;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.portal.LzyPortalApi;
import com.google.protobuf.ByteString;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class SnapshotInputSlot extends LzyInputSlotBase implements SnapshotSlot {
    private static final Logger LOG = LogManager.getLogger(SnapshotInputSlot.class);
    private static final ThreadGroup READER_TG = new ThreadGroup("input-slot-readers");

    private final PortalService portalService;
    private final LzyPortal.PortalSlotDesc.Snapshot snapshotData;
    private final SnapshotEntry snapshot;
    private final StorageClient storageClient;
    private final OutputStream outputStream;

    private final ExecutorService s3UploadPool;
    private final Runnable slotSyncHandler;
    private volatile SnapshotSlotStatus state = SnapshotSlotStatus.INITIALIZING;
    private volatile Future<?> s3UploadFuture;

    public SnapshotInputSlot(PortalService portalService, LzyPortal.PortalSlotDesc.Snapshot snapshotData,
                             SlotInstance slotData, SnapshotEntry snapshot, StorageClient storageClient,
                             ExecutorService s3UploadPool, @Nullable Runnable syncHandler)
        throws IOException
    {
        super(slotData);
        this.portalService = portalService;
        this.snapshotData = snapshotData;
        this.snapshot = snapshot;
        this.storageClient = storageClient;
        this.outputStream = Files.newOutputStream(snapshot.getTempfile());
        this.s3UploadPool = s3UploadPool;
        this.slotSyncHandler = syncHandler;
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> dataProvider) {
        snapshot.getState().set(SnapshotEntry.State.PREPARING);
        super.connect(slotUri, dataProvider);
        LOG.info("Attempt to connect to " + slotUri + " slot " + this);

        onState(LMS.SlotStatus.State.OPEN, () -> {
            try {
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                LOG.error("Error while closing file {}: {}", snapshot.getTempfile(), e.getMessage(), e);
            }
        });

        onState(LMS.SlotStatus.State.SUSPENDED, () -> {
            try {
                portalService.openSlots(LzyPortalApi.OpenSlotsRequest.newBuilder()
                    .addSlots(LzyPortal.PortalSlotDesc.newBuilder()
                        .setSnapshot(snapshotData)
                        .setChannelId(instance().channelId())
                        .setSlot(LMS.Slot.newBuilder()
                            .setName(definition().name() + "_out_" + UUID.randomUUID())
                            .setContentType(ProtoConverter.toProto(definition().contentType()))
                            .setMedia(ProtoConverter.toProto(definition().media()))
                            .setDirection(LMS.Slot.Direction.OUTPUT)
                            .build())
                        .build())
                    .build());
            } catch (CreateSlotException | NotImplementedException e) {
                LOG.error("Portal cannot assign as sender for data: { storageUri: {}, error: {} }",
                    snapshot.getStorageUri().toString(), e.getMessage(), e);
            }
        });

        var t = new Thread(READER_TG, () -> {
            // read all data to local storage (file), then OPEN the slot
            readAll();
            snapshot.getState().set(SnapshotEntry.State.DONE);

            state = SnapshotSlotStatus.SYNCING;

            // store local snapshot to S3
            s3UploadFuture = s3UploadPool.submit(new Runnable() {
                @Override
                public String toString() {
                    return SnapshotInputSlot.this.toString();
                }

                @Override
                public void run() {
                    try {
                        storageClient.write(snapshot.getStorageUri(), snapshot.getTempfile());
                        state = SnapshotSlotStatus.SYNCED;
                        if (slotSyncHandler != null) {
                            slotSyncHandler.run();
                        }
                    } catch (Exception e) {
                        LOG.error("Error while storing slot '{}' content in s3 storage: {}", name(), e.getMessage(), e);
                        state = SnapshotSlotStatus.FAILED;
                    }

                }
            });

            suspend();
            synchronized (snapshot) {
                snapshot.notifyAll();
            }
        }, "reader-from-" + slotUri + "-to-" + definition().name());
        t.start();

        onState(LMS.SlotStatus.State.DESTROYED, t::interrupt);
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        super.onChunk(bytes);
        outputStream.write(bytes.toByteArray());
    }

    @Override
    public void destroy(@Nullable String error) {
        if (error != null && s3UploadFuture != null) {
            LOG.warn("Terminate S3 upload for slot `{}` by reason: {}", this, error);
            s3UploadFuture.cancel(true);
        }

        super.destroy(error);
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.warn("Can not close storage for {}: {}", this, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "SnapshotInputSlot: " + definition().name() + " -> " + snapshot.getTempfile().toString();
    }

    @Override
    public SnapshotSlotStatus snapshotState() {
        return state;
    }
}
