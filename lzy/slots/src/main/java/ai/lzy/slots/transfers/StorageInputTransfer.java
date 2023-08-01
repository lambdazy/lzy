package ai.lzy.slots.transfers;

import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.storage.StorageConfig;
import ai.lzy.storage.StorageConfig.AzureBlobStorageCredentials;
import ai.lzy.storage.StorageConfig.S3Credentials;
import ai.lzy.v1.common.LC;

import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

public class StorageInputTransfer implements InputTransfer {
    private final LC.PeerDescription peer;
    private final StorageClient client;

    public StorageInputTransfer(LC.PeerDescription peer, StorageClientFactory factory) {
        this.peer = peer;

        if (peer.getStoragePeer().hasAzure()) {
            var azure = new AzureBlobStorageCredentials(peer.getStoragePeer().getAzure().getConnectionString());
            this.client = factory.provider(StorageConfig.of(azure)).get();
        } else {
            var s3proto = peer.getStoragePeer().getS3();
            var s3 = new S3Credentials(s3proto.getEndpoint(), s3proto.getAccessToken(), s3proto.getSecretToken());
            this.client = factory.provider(StorageConfig.of(s3)).get();
        }
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
