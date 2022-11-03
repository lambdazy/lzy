package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.grpc.ProtoValidator;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.channelmanager.v2.control.ChannelController;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.channelmanager.v2.db.OperationStorage;
import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

    private final ChannelManagerDataSource dataSource;
    private final ChannelStorage channelStorage;
    private final OperationStorage operationStorage;
    private final ChannelController channelController;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerService(ChannelManagerDataSource dataSource, GrainedLock lockManager,
                                 ChannelStorage channelStorage, OperationStorage operationStorage, ChannelController channelController)
    {
        this.dataSource = dataSource;
        this.channelStorage = channelStorage;
        this.operationStorage = operationStorage;
        this.channelController = channelController;
        this.lockManager = lockManager;
    }

    @Override
    public void bind(LCMS.BindRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Bind slot to channel failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        final String slotUri = request.getSlotInstance().getSlotUri();
        final String channelId = request.getSlotInstance().getChannelId();
        String operationDescription = "Bind %s slot %s to channel %s"
            .formatted(request.getSlotOwner(), slotUri, channelId);
        LOG.info(operationDescription);

        // reject  Portal <-> Portal

        final Endpoint endpoint = Endpoint.fromProto(request.getSlotInstance(), request.getSlotOwner());
        try (final var guard = lockManager.withLock(channelId)) {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.insertBindingEndpoint(endpoint, null));
        } catch (AlreadyExistsException e) {
            LOG.error(operationDescription + " failed, {}", e.getMessage());
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).asException());
            return;
        } catch (NotFoundException e) {
            LOG.error("Bind {} slot={} to channel={} failed, "
                      + "{}", request.getSlotOwner(), slotUri, channelId, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Bind {} slot={} to channel={} failed, "
                      + "got exception: {}", request.getSlotOwner(), slotUri, channelId, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation;
        try {
            operation = withRetries(defaultRetryPolicy(), LOG, () -> operationStorage.create(
                    operationDescription, "ChannelManager", Any.pack(LCMS.BindMetadata.getDefaultInstance()), null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(operation.toProto());
        responseObserver.onCompleted();

        LOG.info(operationDescription + " responded, async operation started");
        channelController.executeBind(endpoint);

        /*

        // move in other thread
            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel == null) {
                String errorMessage = "Channel with id " + channelId + " not found";
                LOG.error("Bind {} slot={} to channel={} failed, "
                          + "channel not found", request.getSlotOrigin(), slotUri, channelId);
                //responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }

            channelController.executeBind(channel, endpoint);
            /* add slot with edges, for each edge:

                // request slot-api to connect, save operation (or mark edge as connecting)

                // polling operation, waiting for connect

                // mark operation completed and mark edge connected in one transaction


            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.markEndpointBound(endpoint.uri().toString(), null));

            LOG.info("Bind {} slot={} to channel={} done", request.getSlotOrigin(), slotUri, channelId);
        } catch (Exception e) {
            LOG.error("Bind {} slot={} to channel={} failed, "
                      + "got exception: {}", request.getSlotOrigin(), slotUri, channelId, e.getMessage(), e);
            //responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }
        */
    }

    @Override
    public void unbind(LCMS.UnbindRequest request, StreamObserver<LongRunning.Operation> responseObserver) {
        if (!ProtoValidator.isValid(request)) {
            LOG.error("Unbind slot failed, invalid argument");
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());
            return;
        }

        String operationDescription = "Unbind slot %s".formatted(request.getSlotUri());
        LOG.info(operationDescription);

        final String slotUri = request.getSlotUri();
        final Endpoint endpoint;
        try {
            endpoint = channelStorage.getEndpoint(slotUri, null);
        } catch (NotFoundException e) {
            LOG.error("Unbind slot {} failed, {}",  slotUri, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Unbind slot {} failed, got exception: {}", slotUri, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        try (final var guard = lockManager.withLock(endpoint.channelId())) {
            withRetries(defaultRetryPolicy(), LOG, () -> channelStorage.markEndpointUnbinding(slotUri, null));
        }  catch (NotFoundException e) {
            LOG.error("Unbind slot {} failed, {}",  slotUri, e.getMessage(), e);
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Unbind slot {} failed, got exception: {}", slotUri, e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        final Operation operation;
        try {
            operation = withRetries(defaultRetryPolicy(), LOG, () -> operationStorage.create(
                operationDescription, "ChannelManager", Any.pack(LCMS.UnbindMetadata.getDefaultInstance()), null));
        } catch (Exception e) {
            LOG.error(operationDescription + " failed, cannot create operation", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        responseObserver.onNext(operation.toProto());
        responseObserver.onCompleted();


        LOG.info(operationDescription + " responded, async operation started");
        channelController.executeUnbind(endpoint);
/*
        try (final var guard = lockManager.withLock(channelId)) {
            final Channel channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel == null) {
                String errorMessage = "Channel with id " + channelId + " not found";
                //LOG.error("Bind {} slot={} to channel={} failed, "
                //          + "channel not found", request.getSlotOrigin(), slotUri, channelId);
                //responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
                return;
            }

            channelController.executeUnbind(channel, endpoint);
            /* remove slot with edges, for each edge:

                // request slot-api to disconnect

                // mark edge as disconnected



            withRetries(defaultRetryPolicy(), LOG, () ->
                channelStorage.removeEndpointWithConnections(endpoint.uri().toString(), null));
        } catch (Exception e) {
            LOG.error("Unbind slot {} failed, got exception: {}", slotUri, e.getMessage(), e);
            //responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }


*/
    }
}
