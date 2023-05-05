package ai.lzy.service.workflow.start;

import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
public class AllocPortalVmSpec {
    String workflowName;
    String executionId;
    String portalId;
    String sessionId;
    String dockerImage;
    String poolZone;
    String poolLabel;
    String privateKey;
    int portalPort;
    int slotsApiPort;
    int workersPoolSize;
    int downloadPoolSize;
    int chunksPoolSize;
    Duration allocationTimeout;
    Duration allocateVmCacheTimeout;
    boolean createStdChannels;
    String stdoutChannelId;
    String stderrChannelId;
    String channelManagerAddress;
    String iamAddress;
    String whiteboardAddress;
}
