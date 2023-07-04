package ai.lzy.fs.transfers;

import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMST;

import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class StorageOutputTransfer implements OutputTransfer {
    private final LC.PeerDescription peer;
    private final StorageClient client;

    public StorageOutputTransfer(LC.PeerDescription peer, StorageClientFactory clientFactory) {
        this.peer = peer;

        var storageConfigBuilder = LMST.StorageConfig.newBuilder();
        if (peer.getStoragePeer().hasAzure()) {
            storageConfigBuilder.setAzure(peer.getStoragePeer().getAzure());
        } else {
            storageConfigBuilder.setS3(peer.getStoragePeer().getS3());
        }

        this.client = clientFactory.provider(storageConfigBuilder.build()).get();
    }

    @Override
    public void readFrom(ReadableByteChannel source) {
        try {
            var uri = URI.create(peer.getStoragePeer().getStorageUri());

            client.write(uri, Channels.newInputStream(source));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
