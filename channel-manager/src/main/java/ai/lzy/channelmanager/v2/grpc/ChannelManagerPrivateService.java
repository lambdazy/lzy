package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.channel.v2.Channel;
import ai.lzy.channelmanager.grpc.ProtoConverter;
import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.v1.channel.v2.LCM;
import ai.lzy.v1.channel.v2.LCMPS;
import ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerPrivateService extends LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerPrivateService.class);

    private final ChannelStorage channelStorage;

    @Inject
    public ChannelManagerPrivateService(ChannelStorage channelStorage) {
        this.channelStorage = channelStorage;
    }


    @Override
    public void create(LCMPS.ChannelCreateRequest request,
                       StreamObserver<LCMPS.ChannelCreateResponse> responseObserver)
    {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Create channel failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        LOG.info("Create channel {}", request.getChannelSpec().getChannelName());

        final String executionId = request.getExecutionId();
        final LCM.ChannelSpec channelSpec = request.getChannelSpec();
        final String channelName = channelSpec.getChannelName();
        final String channelId = String.join("-", "channel", executionId).replaceAll("[^a-zA-z0-9-]+", "-")
                                 + "!" + channelName;

        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.insertChannel(channelId, executionId, ProtoConverter.fromProto(channelSpec), null));
        } catch (Exception e) {
            LOG.error("Create channel {} failed, got exception: {}", channelName, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }

        LOG.info("Create channel {} done, channelId={}", channelName, channelId);
        responseObserver.onCompleted();
    }

    @Override
    public void destroy(LCMPS.ChannelDestroyRequest request,
                        StreamObserver<LongRunning.Operation> responseObserver)
    {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Destroy channel failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        LOG.info("Destroy channel {}", request.getChannelId());

        final String channelId = request.getChannelId();
        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.setChannelLifeStatusDestroying(channelId, null));
        } catch (Exception e) {
            LOG.error("Destroy channel {} failed, got exception: {}", channelId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Channel channel;
        try {
            channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
        } catch (Exception e) {
            LOG.error("Destroy channel {} failed, got exception: {}", channelId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        // create and return operation

        if (channel == null) {
            LOG.warn("Destroy channel {} skipped, channel not found", channelId);
        } else {
            // destroy channel ?? more statuses
        }

        // mark operation done
    }

    @Override
    public void destroyAll(LCMPS.ChannelDestroyAllRequest request,
                           StreamObserver<LongRunning.Operation> responseObserver)
    {

        if (!ProtoValidator.isValid(request)) {
            LOG.error("Destroying all channels for execution failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        LOG.info("Destroying all channels for execution {}", request.getExecutionId());

        final String executionId = request.getExecutionId();
        try {
            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.setChannelLifeStatusDestroyingOfExecution(executionId, null));
        } catch (Exception e) {
            LOG.error("Destroying all channels for execution {} failed, "
                      + "got exception: {}", executionId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        List<Channel> channels;
        try {
            channels = channelStorage.listChannels(executionId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
        } catch (Exception e) {
            LOG.error("Destroying all channels for execution {} failed, "
                      + "got exception: {}", executionId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        // create and return operation

        for (final Channel channel : channels) {

            // destroy channel ?? more statuses

            // change operation metadata, add deleted channel

        }

        // mark operation done
    }

    @Override
    public void status(LCMPS.ChannelStatusRequest request,
                       StreamObserver<LCMPS.ChannelStatusResponse> responseObserver)
    {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Get status for channel failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        LOG.info("Get status for channel {}", request.getChannelId());

        final String channelId = request.getChannelId();
        final Channel channel;
        try {
            channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
        } catch (Exception e) {
            LOG.error("Get status for channel {} failed, got exception: {}", channelId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        if (channel == null) {
            LOG.error("Get status for channel {} failed, channel not found", request.getChannelId());
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }

        responseObserver.onNext(ProtoConverter.createChannelStatusResponse(channel));
        LOG.info("Get status for channel {} done", channelId);
        responseObserver.onCompleted();
    }

    @Override
    public void statusAll(LCMPS.ChannelStatusAllRequest request,
                          StreamObserver<LCMPS.ChannelStatusAllResponse> responseObserver)
    {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Get status for channels of execution failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        LOG.info("Get status for channels of execution {}", request.getExecutionId());

        final String executionId = request.getExecutionId();
        List<Channel> channels;
        try {
            channels = channelStorage.listChannels(executionId, ChannelStorage.ChannelLifeStatus.DESTROYING, null);
        } catch (Exception e) {
            LOG.error("Get status for channels of execution {} failed, "
                      + "got exception: {}", executionId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(ProtoConverter.createChannelStatusAllResponse(channels));
        LOG.info("Get status for channels of execution {} done", executionId);
        responseObserver.onCompleted();
    }
}
