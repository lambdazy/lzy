package ai.lzy.storage.impl;

import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.storage.LSS.CreateS3BucketRequest;
import ai.lzy.v1.storage.LSS.CreateS3BucketResponse;
import ai.lzy.v1.storage.LSS.DeleteS3BucketRequest;
import ai.lzy.v1.storage.LSS.DeleteS3BucketResponse;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MockS3Storage extends LzyStorageServiceGrpc.LzyStorageServiceImplBase {
    private final Map<String, LMS3.AmazonS3Endpoint> buckets = new ConcurrentHashMap<>();

    public Map<String, LMS3.AmazonS3Endpoint> getBuckets() {
        return buckets;
    }

    @Override
    public void createS3Bucket(CreateS3BucketRequest request, StreamObserver<CreateS3BucketResponse> response) {
        var creds = LMS3.AmazonS3Endpoint.newBuilder()
            .setEndpoint("localhost:32000")
            .setAccessToken(UUID.randomUUID().toString())
            .setSecretToken(UUID.randomUUID().toString())
            .build();

        var prev = buckets.putIfAbsent(request.getBucket(), creds);
        if (prev != null) {
            response.onError(Status.ALREADY_EXISTS.asException());
            return;
        }

        response.onNext(CreateS3BucketResponse.newBuilder()
            .setAmazon(creds)
            .build());
        response.onCompleted();
    }

    @Override
    public void deleteS3Bucket(DeleteS3BucketRequest request, StreamObserver<DeleteS3BucketResponse> response) {
        var creds = buckets.remove(request.getBucket());

        if (creds == null) {
            response.onError(Status.NOT_FOUND.asException());
            return;
        }

        response.onNext(DeleteS3BucketResponse.getDefaultInstance());
        response.onCompleted();
    }
}
