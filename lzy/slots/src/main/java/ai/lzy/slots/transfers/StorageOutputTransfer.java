package ai.lzy.slots.transfers;

import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.storage.StorageConfig;
import ai.lzy.storage.StorageConfig.AzureBlobStorageCredentials;
import ai.lzy.storage.StorageConfig.S3Credentials;
import ai.lzy.v1.common.LC;

import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class StorageOutputTransfer implements OutputTransfer {
    private final LC.PeerDescription peer;
    private final StorageClient client;

    public StorageOutputTransfer(LC.PeerDescription peer, StorageClientFactory clientFactory) {
        this.peer = peer;

        if (peer.getStoragePeer().hasAzure()) {
            var azure = new AzureBlobStorageCredentials(peer.getStoragePeer().getAzure().getConnectionString());
            this.client = clientFactory.provider(StorageConfig.of(azure)).get();
        } else {
            var s3proto = peer.getStoragePeer().getS3();
            var s3 = new S3Credentials(s3proto.getEndpoint(), s3proto.getAccessToken(), s3proto.getSecretToken());
            this.client = clientFactory.provider(StorageConfig.of(s3)).get();
        }
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
