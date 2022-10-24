package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.channel.SlotEndpoint;
import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.db.ChannelStorage;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
public class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

    private final ChannelManagerDataSource dataSource;
    private final ChannelStorage channelStorage;
    private final GrainedLock lockManager;

    @Inject
    public ChannelManagerService(ChannelManagerDataSource dataSource,
                                 ChannelStorage channelStorage, GrainedLock lockManager)
    {
        this.dataSource = dataSource;
        this.channelStorage = channelStorage;
        this.lockManager = lockManager;
    }

    @Override
    public void bind(LCMS.BindRequest attach,
                     StreamObserver<LCMS.BindResponse> responseObserver)
    {
        LOG.info("Bind slot={} to channel={}",
            attach.getSlotInstance().getSlot().getName(),
            attach.getSlotInstance().getChannelId());

        if (!ProtoValidator.isValid(attach.getSlotInstance())) {
            String errorMessage = "Request shouldn't contain empty fields";
            LOG.error("Bind slot={} to channel={} failed, invalid argument: {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                errorMessage);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            return;
        }

        final SlotInstance slotInstance = ProtoConverter.fromProto(attach.getSlotInstance());
        final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
        final String channelId = endpoint.slotInstance().channelId();

        Channel channel;

        try (var guard = lockManager.withLock(attach.getSlotInstance().getChannelId())) {
            channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel != null) {
                final Stream<Endpoint> newBound = channel.bind(endpoint);

                final Map<Endpoint, Endpoint> addedEdges = switch (endpoint.slotSpec().direction()) {
                    case OUTPUT -> newBound.collect(Collectors.toMap(e -> endpoint, e -> e));
                    case INPUT -> newBound.collect(Collectors.toMap(e -> e, e -> endpoint));
                };

                withRetries(defaultRetryPolicy(), LOG, () -> {
                    try (final var transaction = TransactionHandle.create(dataSource)) {
                        channelStorage.insertEndpoint(endpoint, transaction);
                        channelStorage.insertEndpointConnections(channelId, addedEdges, transaction);
                        transaction.commit();
                    }
                });
            }
        } catch (ChannelException e) {
            LOG.error("Bind slot={} to channel={} failed, invalid argument: {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Bind slot={} to channel={} failed, got exception: {}",
                attach.getSlotInstance().getSlot().getName(),
                attach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            // TODO: unbind channel
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        if (channel != null) {
            responseObserver.onNext(LCMS.BindResponse.getDefaultInstance());
            LOG.info("Bind slot={} to channel={} done",
                attach.getSlotInstance().getSlot().getName(), attach.getSlotInstance().getChannelId());
            responseObserver.onCompleted();
        } else {
            String errorMessage = "Channel with id " + channelId + " not found";
            LOG.error("Bind slot={} to channel={} failed, channel not found",
                attach.getSlotInstance().getSlot().getName(), attach.getSlotInstance().getChannelId());
            responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
            // TODO: unbind channel
        }
    }

    @Override
    public void unbind(LCMS.UnbindRequest detach,
                       StreamObserver<LCMS.UnbindResponse> responseObserver)
    {
        LOG.info("Unbind slot={} to channel={}",
            detach.getSlotInstance().getSlot(),
            detach.getSlotInstance().getChannelId());

        if (!ProtoValidator.isValid(detach.getSlotInstance())) {
            String errorMessage = "Request shouldn't contain empty fields";
            LOG.error("Unbind slot={} to channel={} failed, invalid argument: {}",
                detach.getSlotInstance().getSlot().getName(),
                detach.getSlotInstance().getChannelId(),
                errorMessage);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
            return;
        }

        final SlotInstance slotInstance = ProtoConverter.fromProto(detach.getSlotInstance());
        final Endpoint endpoint = SlotEndpoint.getInstance(slotInstance);
        final String channelId = endpoint.slotInstance().channelId();

        Channel channel;

        try (var guard = lockManager.withLock(detach.getSlotInstance().getChannelId())) {
            channel = channelStorage.findChannel(channelId, ChannelStorage.ChannelLifeStatus.ALIVE, null);
            if (channel != null) {
                channel.unbind(endpoint);
                withRetries(defaultRetryPolicy(), LOG,
                    () -> channelStorage.removeEndpointWithConnections(endpoint, null));
            }
        } catch (ChannelException e) {
            LOG.error("Unbind slot={} to channel={} failed, invalid argument: {}",
                detach.getSlotInstance().getSlot().getName(),
                detach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        } catch (Exception e) {
            LOG.error("Unbind slot={} to channel={} failed, got exception: {}",
                detach.getSlotInstance().getSlot().getName(),
                detach.getSlotInstance().getChannelId(),
                e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
            return;
        }

        if (channel != null) {
            responseObserver.onNext(LCMS.UnbindResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            String errorMessage = "Channel with id " + channelId + " not found";
            LOG.error("Unbind slot={} to channel={} failed, channel not found",
                detach.getSlotInstance().getSlot().getName(), detach.getSlotInstance().getChannelId());
            responseObserver.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
        }
    }

}
