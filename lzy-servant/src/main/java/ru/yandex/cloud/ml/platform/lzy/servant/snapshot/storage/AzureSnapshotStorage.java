package ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.azure.blobstorage.AzureTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

public class AzureSnapshotStorage implements SnapshotStorage {

    private static final Logger LOG = LogManager.getLogger(AzureSnapshotStorage.class);
    private final BlobServiceClient client;
    private final Transmitter transmitter;

    public AzureSnapshotStorage(String connectionString, String transmitterName, int downloadsPoolSize,
                                int chunksPoolSize) {
        client = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        transmitter = new AzureTransmitterFactory(client).fixedPoolsTransmitter(transmitterName, downloadsPoolSize,
            chunksPoolSize);
    }

    public AzureSnapshotStorage(Lzy.AzureCredentials credentials, String transmitterName, int downloadsPoolSize,
                                int chunksPoolSize) {
        this(credentials.getConnectionString(), transmitterName, downloadsPoolSize, chunksPoolSize);
    }

    public AzureSnapshotStorage(Lzy.AzureSASCredentials credentials, String transmitterName, int downloadsPoolSize,
                                int chunksPoolSize) {
        client = new BlobServiceClientBuilder()
            .endpoint(credentials.getEndpoint())
            .buildClient();
        transmitter = new AzureTransmitterFactory(client).fixedPoolsTransmitter(transmitterName, downloadsPoolSize,
            chunksPoolSize);
    }

    @Override
    public Transmitter transmitter() {
        return transmitter;
    }

    @Override
    public URI getURI(String bucketName, String key) {
        try {
            return new URIBuilder().setScheme("azure").setPath(Path.of(bucketName, key).toString()).build();
        } catch (URISyntaxException e) {
            LOG.info(e);
            return null;
        }
    }

    @Override
    public String bucket(URI uri) {
        Path path = Path.of(uri.getPath());
        return path.getParent().toString().substring(1);
    }

    @Override
    public String key(URI uri) {
        Path path = Path.of(uri.getPath());
        return path.getFileName().toString();
    }
}
