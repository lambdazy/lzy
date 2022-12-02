package ai.lzy.channelmanager.v2.slot;

import ai.lzy.channelmanager.v2.model.Endpoint;
import ai.lzy.v1.longrunning.LongRunning;

public interface SlotApiClient {
    
    LongRunning.Operation connect(Endpoint sender, Endpoint receiver);

    String connectGetOp(Endpoint sender, Endpoint receiver, String operationId);

    void disconnect(Endpoint endpoint);

    void destroy(Endpoint endpoint);

}
