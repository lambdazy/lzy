package ai.lzy.channelmanager.v2.slot;

import ai.lzy.channelmanager.v2.model.Endpoint;

import java.time.Duration;

public interface SlotApiClient {
    
    String connectStart(Endpoint sender, Endpoint receiver);
    void connectFinish(Endpoint sender, Endpoint receiver, Duration timeout, String operationId);

    void disconnect(Endpoint endpoint);

    void destroy(Endpoint endpoint);

}
