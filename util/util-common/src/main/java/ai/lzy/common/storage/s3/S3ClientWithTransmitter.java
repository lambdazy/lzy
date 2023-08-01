package ai.lzy.common.storage.s3;

import ai.lzy.common.storage.StorageClientWithTransmitter;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.util.concurrent.MoreExecutors;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ExecutorService;


public final class S3ClientWithTransmitter extends StorageClientWithTransmitter {
    private final AmazonS3 amazonS3Client;
    private final Transmitter transmitter;

    public S3ClientWithTransmitter(String endpoint, String accessToken, String secretToken,
                                   int byteBufferPoolSize, ExecutorService transferPool,
                                   ExecutorService chunkPool, ExecutorService consumerPool)
    {
        this.amazonS3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessToken, secretToken)))
            .withEndpointConfiguration(new AmazonS3ClientBuilder.EndpointConfiguration(endpoint, "us-west-1"))
            .withPathStyleAccessEnabled(true).build();

        this.transmitter = new AmazonTransmitterFactory(amazonS3Client) {
            Transmitter withUserThreadPool() {
                return create(
                    createByteBufferPool(DEFAULT_TRANSMITTER_NAME, byteBufferSizeType, byteBufferPoolSize),
                    MoreExecutors.listeningDecorator(transferPool),
                    MoreExecutors.listeningDecorator(chunkPool),
                    MoreExecutors.listeningDecorator(consumerPool)
                );
            }
        }.withUserThreadPool();
    }

    @Override
    protected Transmitter transmitter() {
        return transmitter;
    }

    @Override
    protected DownloadRequest downloadRequest(URI uri) {
        var amazonUri = new AmazonS3URI(uri);
        return new DownloadRequestBuilder().bucket(amazonUri.getBucket())
            .key(amazonUri.getKey()).build();
    }

    @Override
    protected UploadRequest uploadRequest(URI uri, InputStream source) {
        var amazonUri = new AmazonS3URI(uri);
        return new UploadRequestBuilder().bucket(amazonUri.getBucket()).key(amazonUri.getKey())
            .stream(() -> new BufferedInputStream(source)).build();
    }

    @Override
    public boolean blobExists(URI uri) {
        var amazonUri = new AmazonS3URI(uri);
        return amazonS3Client.doesObjectExist(amazonUri.getBucket(), amazonUri.getKey());
    }
}
