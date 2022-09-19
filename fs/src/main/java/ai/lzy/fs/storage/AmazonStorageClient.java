package ai.lzy.fs.storage;

import ai.lzy.v1.deprecated.Lzy;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

public class AmazonStorageClient implements StorageClient {

    private static final Logger LOG = LogManager.getLogger(AzureStorageClient.class);
    private final AmazonS3 client;
    private final Transmitter transmitter;

    public AmazonStorageClient(String accessToken, String secretToken, URI endpoint, String transmitterName,
                                 int downloadsPoolSize, int chunksPoolSize) {
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
        transmitter = new AmazonTransmitterFactory(client).fixedPoolsTransmitter(transmitterName, downloadsPoolSize,
            chunksPoolSize);
    }

    public AmazonStorageClient(Lzy.AmazonCredentials credentials, String transmitterName, int downloadsPoolSize,
                                 int chunksPoolSize) {
        this(credentials.getAccessToken(), credentials.getSecretToken(), URI.create(credentials.getEndpoint()),
            transmitterName, downloadsPoolSize, chunksPoolSize);
    }

    @Override
    public Transmitter transmitter() {
        return transmitter;
    }

    @Override
    public URI getURI(String bucketName, String key) {
        try {
            return new URIBuilder(client.getUrl(bucketName, key).toString())
                .setScheme("s3").setPath(Path.of(bucketName, key).toString()).build();
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
