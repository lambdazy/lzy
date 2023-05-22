package ai.lzy.service.config;

public record PortalVmSpec(String poolZone, String poolLabel, String dockerImage, String privateKey,
                           int portalPort, int slotsApiPort, int workersPoolSize, int downloadPoolSize,
                           int chunksPoolSize, String channelManagerAddress, String iamAddress,
                           String whiteboardAddress) {}