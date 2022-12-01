package ai.lzy.channelmanager.v2.operation.state;

import javax.annotation.Nullable;

public class BindActionState {
    private final String channelId;
    private final String endpointUri;

    @Nullable
    private String connectingEndpointUri;
    @Nullable
    private String connectOperationId;

    public BindActionState(String channelId, String endpointUri,
                           @Nullable String connectingEndpointUri,
                           @Nullable String connectOperationId)
    {
        this.channelId = channelId;
        this.endpointUri = endpointUri;
        this.connectingEndpointUri = connectingEndpointUri;
        this.connectOperationId = connectOperationId;
    }

    public String channelId() {
        return channelId;
    }

    public String endpointUri() {
        return endpointUri;
    }

    @Nullable
    public String connectingEndpointUri() {
        return connectingEndpointUri;
    }

    @Nullable
    public String connectOperationId() {
        return connectOperationId;
    }

    public BindActionState setConnectingEndpointUri(String connectingEndpointUri) {
        this.connectingEndpointUri = connectingEndpointUri;
        return this;
    }

    public BindActionState setConnectOperationId(String connectOperationId) {
        this.connectOperationId = connectOperationId;
        return this;
    }

    public BindActionState reset() {
        this.connectingEndpointUri = null;
        this.connectOperationId = null;
        return this;
    }

}
