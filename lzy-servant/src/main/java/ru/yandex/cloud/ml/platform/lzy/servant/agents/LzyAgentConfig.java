package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import lombok.Builder;
import lombok.Value;

import java.net.URI;
import java.nio.file.Path;

@Value
@Builder
public class LzyAgentConfig {
    URI serverAddress;
    String agentName;
    String agentInternalName;
    String token;
    Path root;
    String tokenSign;
    String user;
    String task;
    int agentPort;
}
