package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.grpc.SlotConnectionManager;
import ai.lzy.channelmanager.operation.ChannelOperationExecutor;
import ai.lzy.model.db.DbHelper;
import ai.lzy.v1.slots.v2.LSA;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Singleton
public class StartTransmissionAction {
    private static final Logger LOG = LogManager.getLogger(StartTransmissionAction.class);

    private final TransmissionsDao connections;

    private final SlotConnectionManager connectionManager;
    private final ChannelOperationExecutor operationExecutor;

    public StartTransmissionAction(TransmissionsDao connections, SlotConnectionManager connectionManager,
                                   ChannelOperationExecutor operationExecutor)
    {
        this.connections = connections;
        this.connectionManager = connectionManager;
        this.operationExecutor = operationExecutor;
    }

    public void schedule(Peer producer, Peer consumer) {
        operationExecutor.schedule(() -> run(producer, consumer), 0L, TimeUnit.SECONDS);
    }

    private void run(Peer producer, Peer consumer) {

        try {
            var url = consumer.peerDescription().getSlotPeer().getPeerUrl();

            var uri = new URI(url);

            var connection = connectionManager.getConnection(uri);

            connection.v2SlotsApi().connectPeer(
                LSA.ConnectPeerRequest.newBuilder()
                    .setPeerId(consumer.id())
                    .setTarget(producer.peerDescription())
                    .build()
            );

            DbHelper.withRetries(LOG, () -> connections.dropPendingConnection(consumer.id(), null));
        } catch (Exception e) {
            LOG.error("(Connecting producer: {} to consumer: {}): Cannot connect: ", producer, consumer, e);
            // TODO(artolord) drop channel here and abort workflow
        }

    }
}
