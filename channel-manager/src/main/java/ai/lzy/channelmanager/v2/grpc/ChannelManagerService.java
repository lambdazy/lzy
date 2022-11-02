package ai.lzy.channelmanager.v2.grpc;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.db.ChannelStorage;
import ai.lzy.v1.channel.v2.LCMS;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class ChannelManagerService extends LzyChannelManagerGrpc.LzyChannelManagerImplBase {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerService.class);

    private final ChannelManagerDataSource dataSource;
    private final ChannelStorage channelStorage;

    @Inject
    public ChannelManagerService(ChannelManagerDataSource dataSource, ChannelStorage channelStorage)
    {
        this.dataSource = dataSource;
        this.channelStorage = channelStorage;
    }

    @Override
    public void bind(LCMS.BindRequest request, StreamObserver<LCMS.BindResponse> responseObserver) {
        // validate request

        // get channel

        // insert slot, mark as binding

        // send response

        /* add slot with edges, for each edge:

            // request slot-api to connect, save operation (or mark edge as connecting)

            // polling operation, waiting for connect

            // mark operation completed and mark edge connected in one transaction

        */

        // mark slot as bound

    }

    @Override
    public void unbind(LCMS.UnbindRequest request, StreamObserver<LCMS.UnbindResponse> responseObserver) {
        // validate request

        // get channel

        // mark slot as unbinding

        // send response

        /* remove slot with edges, for each edge:

            // request slot-api to disconnect

            // mark edge as disconnected

        */

        // mark slot as unbound

    }
}
