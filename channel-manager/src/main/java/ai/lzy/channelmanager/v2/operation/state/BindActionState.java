package ai.lzy.channelmanager.v2.operation.state;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public class BindActionState {
    private final String channelId;
    private final String endpointUri;

    @Nullable
    private String connectingEndpointUri;
    @Nullable
    private String connectOperationId;

    @JsonCreator
    public BindActionState(@JsonProperty("channelId") String channelId,
                           @JsonProperty("endpointUri") String endpointUri,
                           @JsonProperty("connectingEndpointUri") @Nullable String connectingEndpointUri,
                           @JsonProperty("connectOperationId") @Nullable String connectOperationId)
    {
        this.channelId = channelId;
        this.endpointUri = endpointUri;
        this.connectingEndpointUri = connectingEndpointUri;
        this.connectOperationId = connectOperationId;
    }

    public static BindActionState copyOf(BindActionState other) {
        return new BindActionState(other.channelId, other.endpointUri,
            other.connectingEndpointUri(),
            other.connectOperationId());
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