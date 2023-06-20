package ai.lzy.fs.transfers;

import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.slots.v2.LSA;
import ai.lzy.v1.slots.v2.LzySlotsApiGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
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
    private int currentOffset;
    private Iterator<LSA.ReadDataChunk> stream;
    private int remainingRetries = 15;

    public SlotInputTransfer(LC.PeerDescription peer, int offset, Supplier<String> jwt) {
        this.peer = peer;
        this.currentOffset = offset;

        this.channel = newGrpcChannel(peer.getSlotPeer().getPeerUrl(), LzySlotsApiGrpc.SERVICE_NAME);
        this.stub = newBlockingClient(LzySlotsApiGrpc.newBlockingStub(channel), "SlotsApi", jwt);
    }

    @Override
    public int readInto(SeekableByteChannel outputStream) throws ReadException, IOException {
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

                remainingRetries--;
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
