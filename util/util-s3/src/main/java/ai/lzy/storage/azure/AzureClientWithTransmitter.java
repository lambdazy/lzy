package ai.lzy.storage.azure;

import ai.lzy.storage.StorageClientWithTransmitter;
import ai.lzy.util.azure.blobstorage.AzureTransmitterFactory;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.google.common.util.concurrent.MoreExecutors;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadRequestBuilder;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadRequestBuilder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ExecutorService;

public final class AzureClientWithTransmitter extends StorageClientWithTransmitter {
    private final BlobServiceClient azureClient;
    private final Transmitter transmitter;

    public AzureClientWithTransmitter(String connectionString, int byteBufferPoolSize, ExecutorService transferPool,
                                      ExecutorService chunkPool, ExecutorService consumerPool)
    {
        this.azureClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        this.transmitter = new AzureTransmitterFactory(azureClient) {
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
        try {
            var azureUrl = BlobUrlParts.parse(uri.toURL());
            return new DownloadRequestBuilder().bucket(azureUrl.getBlobContainerName())
                .key(azureUrl.getBlobName()).build();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    protected UploadRequest uploadRequest(URI uri, InputStream source) {
        try {
            var azureUrl = BlobUrlParts.parse(uri.toURL());
            return new UploadRequestBuilder().bucket(azureUrl.getBlobContainerName()).key(azureUrl.getBlobName())
                .stream(() -> new BufferedInputStream(source)).build();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public boolean blobExists(URI uri) throws IOException {
        var azureUrl = BlobUrlParts.parse(uri.toURL());
        return azureClient.createBlobContainer(azureUrl.getBlobContainerName())
            .getBlobClient(azureUrl.getBlobName()).exists();
    }
}
