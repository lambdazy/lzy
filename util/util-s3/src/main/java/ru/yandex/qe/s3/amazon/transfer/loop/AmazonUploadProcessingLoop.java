package ru.yandex.qe.s3.amazon.transfer.loop;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nullable;
import org.joda.time.DateTime;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.loop.UploadProcessingLoop;
import ru.yandex.qe.s3.transfer.meta.Metadata;
import ru.yandex.qe.s3.transfer.upload.ConcurrencyConflictResolve;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Established by terry on 18.01.16.
 */
@NotThreadSafe
public class AmazonUploadProcessingLoop extends UploadProcessingLoop {

    private final Queue<PartETag> eTags = new ConcurrentLinkedQueue<>();
    private AmazonS3 amazonS3;
    private String multiPartUploadId;

    public AmazonUploadProcessingLoop(AmazonS3 amazonS3, ByteBufferPool byteBufferPool,
        ListeningExecutorService taskExecutor, UploadRequest request, Consumer<UploadState> progressListener,
        Executor notifyExecutor) {
        super(byteBufferPool, taskExecutor, request, progressListener, notifyExecutor);
        this.amazonS3 = amazonS3;
    }

    @Override
    protected void uploadObject(String bucket, String key, Metadata metadata, byte[] buffer, int offset, int length,
        ConcurrencyConflictResolve concurrencyConflictResolve, @Nullable DateTime expirationTime) {
        final PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key,
            new ByteArrayInputStream(buffer, offset, length), AmazonMetadataConverter.from(metadata));
        for (Object acl : metadata.getAclObjects()) {
            if (acl instanceof AccessControlList) {
                putObjectRequest.setAccessControlList((AccessControlList) acl);
            }
            if (acl instanceof CannedAccessControlList) {
                putObjectRequest.setCannedAcl((CannedAccessControlList) acl);
            }
        }
        amazonS3.putObject(putObjectRequest);
    }

    @Override
    protected void initMultiPartUpload(String bucket, String key, Metadata metadata,
        ConcurrencyConflictResolve concurrencyConflictResolve, @Nullable DateTime expirationTime) {
        final InitiateMultipartUploadRequest initRequest =
            new InitiateMultipartUploadRequest(bucket, key, AmazonMetadataConverter.from(metadata));
        for (Object acl : metadata.getAclObjects()) {
            if (acl instanceof AccessControlList) {
                initRequest.setAccessControlList((AccessControlList) acl);
            }
            if (acl instanceof CannedAccessControlList) {
                initRequest.setCannedACL((CannedAccessControlList) acl);
            }
        }
        multiPartUploadId = amazonS3.initiateMultipartUpload(initRequest).getUploadId();
    }

    @Override
    protected void uploadObjectPart(String bucket, String key, int partNumber, long partSize, byte[] buffer, int offset,
        int length) {
        final UploadPartRequest uploadPartRequest = new UploadPartRequest()
            .withBucketName(bucket).withKey(key)
            .withUploadId(multiPartUploadId).withPartNumber(partNumber)
            .withPartSize(partSize)
            .withInputStream(new ByteArrayInputStream(buffer, offset, length));
        eTags.add(amazonS3.uploadPart(uploadPartRequest).getPartETag());
    }

    @Override
    protected void completeUpload(String bucket, String key, Metadata metadata, int partsCount) {
        if (multiPartUploadId != null) {
            amazonS3.completeMultipartUpload(
                new CompleteMultipartUploadRequest(bucket, key, multiPartUploadId, new ArrayList<>(eTags)));
        }
    }

    @Override
    protected void abortUpload(String bucket, String key) {
        if (multiPartUploadId != null) {
            amazonS3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, multiPartUploadId));
        }
    }

    @Override
    protected String errorLogDetails(Throwable throwable) {
        return ErrorLogUtils.errorLogDetails(throwable);
    }
}
