package ai.lzy.slots.transfers;

import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMST;

import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

public class StorageInputTransfer implements InputTransfer {
    private final LC.PeerDescription peer;
    private final StorageClient client;

    public StorageInputTransfer(LC.PeerDescription peer, StorageClientFactory factory) {
        this.peer = peer;

        var builder = LMST.StorageConfig.newBuilder()
            .setUri(peer.getStoragePeer().getStorageUri());

        if (peer.getStoragePeer().hasAzure()) {
            builder.setAzure(peer.getStoragePeer().getAzure());
        } else {
            builder.setS3(peer.getStoragePeer().getS3());
        }

        this.client = factory.provider(builder.build()).get();
    }

    @Override
    public int transferChunkTo(SeekableByteChannel sink) throws ReadException {
        try {
            client.read(URI.create(peer.getStoragePeer().getStorageUri()), Channels.newOutputStream(sink));
            sink.close();
        } catch (Exception e) {
            throw new ReadException("Error while reading from s3", e);
        }

        return -1;
    }
}