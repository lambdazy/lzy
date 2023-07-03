package ai.lzy.fs.transfers;

import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class SlotInputTransfer implements InputTransfer, AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(SlotInputTransfer.class);

    private final LC.PeerDescription peer;
    private final ManagedChannel channel;
    private final LzySlotsApiGrpc.LzySlotsApiBlockingStub stub;
    private long currentOffset;
    private Iterator<LSA.ReadDataChunk> stream;
    private int remainingRetries = 15;

    public SlotInputTransfer(LC.PeerDescription peer, long offset, Supplier<String> jwt) {
        this.peer = peer;
        this.currentOffset = offset;

        this.channel = newGrpcChannel(peer.getSlotPeer().getPeerUrl(), LzySlotsApiGrpc.SERVICE_NAME);
        this.stub = newBlockingClient(LzySlotsApiGrpc.newBlockingStub(channel), "SlotsApi", jwt);
        LOG.info("Created SlotInputTransfer for peer {} at offset {}", peer.getPeerId(), offset);
    }

    @Override
    public int transferChunkTo(SeekableByteChannel outputStream) throws ReadException, IOException {
        LSA.ReadDataChunk chunk = null;
        boolean done = false;

        if (outputStream.position() != currentOffset) {
            outputStream.position(currentOffset);
        }

        while (!done) {
            try {
                if (stream == null) {  // Lazy creation of stream
                    this.stream = stub.read(LSA.ReadDataRequest.newBuilder()
                        .setPeerId(peer.getPeerId())
                        .setOffset(currentOffset)
                        .build());
                }

                if (!stream.hasNext()) {
                    return -1;
                }

                chunk = stream.next();
                done = true;
            } catch (StatusRuntimeException e) {
                LOG.warn("Error while reading from slot peer {}, will be retried... ", peer.getPeerId(), e);

                if (!GrpcUtils.retryableStatusCode(e.getStatus())) {
                    LOG.error("Error is not retryable, will not retry", e);
                    throw new ReadException("Cannot read from slot peer " + peer.getPeerId(), e);
                }

                remainingRetries--;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while waiting for retry", ex);
                    throw new IOException("Interrupted while waiting for retry", ex);
                }

                if (remainingRetries == 0) {
                    throw new ReadException("Cannot read from slot peer " + peer.getPeerId(), e);
                }
            }
        }

        if (chunk.hasControl() && chunk.getControl().equals(LSA.ReadDataChunk.Control.EOS)) {
            return -1;
        }

        outputStream.write(chunk.getChunk().asReadOnlyByteBuffer());

        currentOffset += chunk.getChunk().size();
        return chunk.getChunk().size();
    }

    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for channel to terminate", e);
        }
    }
}
