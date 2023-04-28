package ai.lzy.util.azure.blobstorage.transfer;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qe.s3.amazon.transfer.loop.AmazonDownloadProcessingLoop;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.loop.UploadProcessingLoop;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.ConcurrencyConflictResolve;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class AzureUploadProcessingLoop extends UploadProcessingLoop {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonDownloadProcessingLoop.class);
    private final BlobServiceClient client;
    private final ArrayList<String> ids = new ArrayList<>();
    private final ParallelTransferOptions options;

    public AzureUploadProcessingLoop(
        @Nonnull BlobServiceClient client, ParallelTransferOptions options,
        @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService taskExecutor,
        @Nonnull UploadRequest request, @Nullable Consumer<UploadState> progressListener,
        @Nullable Executor notifyExecutor
    ) {
        super(byteBufferPool, taskExecutor, request, progressListener, notifyExecutor);
        this.client = client;
        this.options = options;
    }

    @Override
    protected void uploadObject(String bucket, String key, Metadata metadata, byte[] buffer, int offset, int length,
        ConcurrencyConflictResolve concurrencyConflictResolve, @Nullable DateTime expirationTime) {
        HashMap<String, String> meta = new HashMap<>();
        for (Map.Entry<String, Object> entry : metadata.getMetadata().entrySet()) {
            meta.put(entry.getKey(), entry.getValue().toString());
        }
        try (BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(buffer, offset, length))) {
            client.getBlobContainerClient(bucket).getBlobClient(key).uploadWithResponse(
                in, length, options, null, meta, null, null, null, null
            );
        } catch (IOException e) {
            LOG.error("Error while uploading object", e);
        }

    }

    @Override
    protected void initMultiPartUpload(String bucket, String key, Metadata metadata,
        ConcurrencyConflictResolve concurrencyConflictResolve, @Nullable DateTime expirationTime) {
        ids.clear();
    }

    @Override
    protected void uploadObjectPart(String bucket, String key, int partNumber, long partSize, byte[] buffer, int offset,
        int length) {
        BlockBlobClient blob = client.getBlobContainerClient(bucket).getBlobClient(key).getBlockBlobClient();
        String blockId = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
        blob.stageBlock(blockId, new BufferedInputStream(new ByteArrayInputStream(buffer, offset, length)), length);
        ids.add(blockId);
    }

    @Override
    protected void completeUpload(String bucket, String key, Metadata metadata, int partsCount) {
        BlockBlobClient blob = client.getBlobContainerClient(bucket).getBlobClient(key).getBlockBlobClient();
        HashMap<String, String> meta = new HashMap<>();
        for (Map.Entry<String, Object> entry : metadata.getMetadata().entrySet()) {
            meta.put(entry.getKey(), entry.getValue().toString());
        }
        blob.commitBlockListWithResponse(ids, null, meta, null, null, null, null);
    }

    @Override
    protected void abortUpload(String bucket, String key) {
        BlockBlobClient blob = client.getBlobContainerClient(bucket).getBlobClient(key).getBlockBlobClient();
        BlockList block = blob.listBlocks(BlockListType.ALL);
        blob.commitBlockList(block.getCommittedBlocks().stream().map(Block::getName).collect(Collectors.toList()));
    }

    @Override
    protected String errorLogDetails(Throwable throwable) {
        // TODO make better error logs
        return throwable.toString();
    }
}
