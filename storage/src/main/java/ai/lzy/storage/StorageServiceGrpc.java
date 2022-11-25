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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import static ai.lzy.longrunning.IdempotencyUtils.handleIdempotencyKeyConflict;
import static ai.lzy.longrunning.IdempotencyUtils.loadExistingOp;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class StorageServiceGrpc extends LzyStorageServiceGrpc.LzyStorageServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(StorageServiceGrpc.class);

    private final StorageService service;
    private final OperationDao operationDao;

    private final ExecutorService workersPool;

    public StorageServiceGrpc(StorageService service, @Named(BeanFactory.DAO_NAME) OperationDao operationDao) {
        this.service = service;
        this.operationDao = operationDao;

        this.workersPool = Executors.newFixedThreadPool(8,
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    var th = new Thread(r, "storage-service-worker-" + counter.getAndIncrement());
                    th.setUncaughtExceptionHandler(
                        (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                    return th;
                }
            });
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

        workersPool.submit(() -> service.processCreateBucketOperation(request, op, responseObserver));
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

    public void shutdown() {
        workersPool.shutdown();
    }

    public void shutdownNow() {
        workersPool.shutdownNow();
    }

    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return workersPool.awaitTermination(timeout, timeUnit);
    }
}
