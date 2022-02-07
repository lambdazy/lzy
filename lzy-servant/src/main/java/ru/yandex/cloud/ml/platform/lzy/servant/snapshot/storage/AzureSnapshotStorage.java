package ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.azure.blobstorage.AzureTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;

public class AzureSnapshotStorage implements SnapshotStorage {

    private final BlobServiceClient client;
    private final Transmitter transmitter;
    private final static Logger LOG = LogManager.getLogger(AzureSnapshotStorage.class);

    public AzureSnapshotStorage(String connectionString, String transmitterName, int downloadsPoolSize, int chunksPoolSize){
        client = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        transmitter = new AzureTransmitterFactory(client).fixedPoolsTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize);
    }

    public AzureSnapshotStorage(Lzy.AzureCredentials credentials, String transmitterName, int downloadsPoolSize, int chunksPoolSize){
        this(credentials.getConnectionString(), transmitterName, downloadsPoolSize, chunksPoolSize);
    }

    public AzureSnapshotStorage(Lzy.AzureSASCredentials credentials, String transmitterName, int downloadsPoolSize, int chunksPoolSize){
        client = new BlobServiceClientBuilder()
            .endpoint(credentials.getEndpoint())
            .buildClient();
        transmitter = new AzureTransmitterFactory(client).fixedPoolsTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize);
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
}
