package ai.lzy.service.config;

import ai.lzy.util.auth.credentials.RsaUtils;

public record PortalServiceSpec(String poolZone, String poolLabel, String dockerImage, RsaUtils.RsaKeys rsaKeys,
                                int portalPort, int slotsApiPort, int workersPoolSize, int downloadPoolSize,
                                int chunksPoolSize, String channelManagerAddress, String iamAddress,
                                String whiteboardAddress) {}