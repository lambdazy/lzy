package ai.lzy.fs.storage;

import ai.lzy.util.azure.blobstorage.AzureTransmitterFactory;
import ai.lzy.v1.deprecated.Lzy;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

public class AzureStorageClient implements StorageClient {

    private static final Logger LOG = LogManager.getLogger(AzureStorageClient.class);
    private final BlobServiceClient client;
    private final Transmitter transmitter;

    public AzureStorageClient(String connectionString, String transmitterName, int downloadsPoolSize,
                                int chunksPoolSize) {
        client = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        transmitter = new AzureTransmitterFactory(client).fixedPoolsTransmitter(transmitterName, downloadsPoolSize,
            chunksPoolSize);
    }

    public AzureStorageClient(Lzy.AzureCredentials credentials, String transmitterName, int downloadsPoolSize,
                                int chunksPoolSize) {
        this(credentials.getConnectionString(), transmitterName, downloadsPoolSize, chunksPoolSize);
    }

    public AzureStorageClient(Lzy.AzureSASCredentials credentials, String transmitterName, int downloadsPoolSize,
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
            return new URIBuilder(client.getAccountUrl())
                .setScheme("azure")
                .setPath(Path.of(bucketName, key).toString())
                .build();
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