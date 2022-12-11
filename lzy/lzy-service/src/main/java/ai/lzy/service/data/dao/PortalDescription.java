package ai.lzy.service.data.dao;

import com.google.common.net.HostAndPort;

public record PortalDescription(
    String portalId,
    String vmId,
    HostAndPort vmAddress,
    HostAndPort fsAddress,
    String stdoutChannelId,
    String stderrChannelId,
    PortalStatus portalStatus
) {
    public enum PortalStatus {
        CREATING_STD_CHANNELS, CREATING_SESSION, REQUEST_VM, ALLOCATING_VM, VM_READY
    }
}
