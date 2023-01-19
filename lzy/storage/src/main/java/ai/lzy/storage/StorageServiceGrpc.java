package ai.lzy.storage;

import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.storage.LSS;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class StorageServiceGrpc extends LzyStorageServiceGrpc.LzyStorageServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(StorageServiceGrpc.class);

    private final StorageService service;
    private final OperationDao operationDao;

    public StorageServiceGrpc(StorageService service, @Named("StorageOperationDao") OperationDao operationDao) {
        this.service = service;
        this.operationDao = operationDao;
    }

    @Override
    public void createS3Bucket(LSS.CreateS3BucketRequest request,
                               StreamObserver<LongRunning.Operation> responseObserver)
    {
        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);
        if (idempotencyKey != null && loadExistingOp(operationDao, idempotencyKey, responseObserver, LOG)) {
            return;
        }

        var userId = request.getUserId();
        var bucketName = request.getBucket();

        LOG.info("Create operation for bucket creation: { userId: {}, bucketName: {} }", userId, bucketName);

        final var op = Operation.create(
            userId,
            "Create S3 bucket: name=" + bucketName,
            /* deadline */ null,
            idempotencyKey,
            null);

        try {
            withRetries(LOG, () -> operationDao.create(op, null));
        } catch (Exception ex) {
            if (idempotencyKey != null &&
                handleIdempotencyKeyConflict(idempotencyKey, ex, operationDao, responseObserver, LOG))
            {
                return;
            }

            LOG.error("Cannot create operation for s3 bucket creation: { bucketName: {}, userId: {} }, error: {}",
                bucketName, userId, ex.getMessage(), ex);
            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
            return;
        }

        service.processCreateBucketOperation(request, op, responseObserver);
    }

    @Override
    public void deleteS3Bucket(LSS.DeleteS3BucketRequest request,
                               StreamObserver<LSS.DeleteS3BucketResponse> responseObserver)
    {
        service.deleteBucket(request, responseObserver);
    }

    @Override
    public void getS3BucketCredentials(LSS.GetS3BucketCredentialsRequest request,
                                       StreamObserver<LSS.GetS3BucketCredentialsResponse> responseObserver)
    {
        service.getBucketCreds(request, responseObserver);
    }
}
