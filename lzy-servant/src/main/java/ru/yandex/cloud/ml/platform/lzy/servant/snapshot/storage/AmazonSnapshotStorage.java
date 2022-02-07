package ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

public class AmazonSnapshotStorage implements SnapshotStorage{

    private final AmazonS3 client;
    private final Transmitter transmitter;
    private final static Logger LOG = LogManager.getLogger(AzureSnapshotStorage.class);

    public AmazonSnapshotStorage(String accessToken, String secretToken, URI endpoint, String transmitterName, int downloadsPoolSize, int chunksPoolSize){
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessToken, secretToken);
        client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(
                new AmazonS3ClientBuilder.EndpointConfiguration(
                    endpoint.toString(), "us-west-1"
                )
            )
            .withPathStyleAccessEnabled(true)
            .build();
        transmitter = new AmazonTransmitterFactory(client).fixedPoolsTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize);
    }

    public AmazonSnapshotStorage(Lzy.AmazonCredentials credentials, String transmitterName, int downloadsPoolSize, int chunksPoolSize){
        this(credentials.getAccessToken(), credentials.getSecretToken(), URI.create(credentials.getEndpoint()), transmitterName, downloadsPoolSize, chunksPoolSize);
    }

    @Override
    public Transmitter transmitter() {
        return transmitter;
    }

    @Override
    public URI getURI(String bucketName, String key) {
        try {
            return new URIBuilder().setScheme("s3").setPath(Path.of(bucketName, key).toString()).build();
        } catch (URISyntaxException e) {
            LOG.info(e);
            return null;
        }
    }
}
