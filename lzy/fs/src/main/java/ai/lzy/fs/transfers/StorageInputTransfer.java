package ai.lzy.fs.transfers;

import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMST;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

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
    public int readInto(OutputStream sink) throws ReadException, IOException {
        try {
            client.read(URI.create(peer.getStoragePeer().getStorageUri()), sink);
        } catch (InterruptedException e) {
            throw new ReadException("Interrupted while reading", e);
        }

        return -1;
    }
}
