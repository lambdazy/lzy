package ai.lzy.service.data.dao;

import com.google.common.net.HostAndPort;

public record PortalDescription(
    String portalId,
    String subjectId,
    String allocatorSessionId,
    String vmId,
    HostAndPort vmAddress,
    HostAndPort fsAddress,
    PortalStatus portalStatus
) {
    public enum PortalStatus {
        CREATING_STD_CHANNELS, CREATING_SESSION, REQUEST_VM, ALLOCATING_VM, VM_READY
    }
}
