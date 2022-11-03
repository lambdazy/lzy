package ai.lzy.channelmanager.v2.slot;

import ai.lzy.channelmanager.v2.model.Endpoint;

import java.time.Duration;

public interface SlotApiClient {
    
    void connect(Endpoint sender, Endpoint receiver, Duration timeout);

    void disconnect(Endpoint endpoint);

    void destroy(Endpoint endpoint);

}
