package ai.lzy.allocator.model;

import ai.lzy.allocator.volume.VolumeClaim;
import ai.lzy.allocator.volume.VolumeRequest;

import java.net.Inet6Address;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public record Vm(
    Spec spec,
    Status status,
    AllocateState allocateState,
    @Nullable RunState runState
) {
    public Vm(Spec spec, Status status, AllocateState allocateState) {
        this(spec, status, allocateState, null);
    }

    @Override
    public String toString() {
        return "<"
            + "sessionId='" + spec.sessionId + '\''
            + ", vmId='" + spec.vmId + '\''
            + ", poolLabel='" + spec.poolLabel + '\''
            + ", state=" + status
            + '>';
    }

    public record Spec(
        String vmId,
        String sessionId,
        String poolLabel,
        String zone,
        List<Workload> initWorkloads,
        List<Workload> workloads,
        List<VolumeRequest> volumeRequests,
        @Nullable Inet6Address proxyV6Address
    ) {
        public Spec withVmId(String vmId) {
            return new Spec(vmId, sessionId, poolLabel, zone, initWorkloads, workloads, volumeRequests, proxyV6Address);
        }
    }

    public record AllocateState(
        String operationId,
        Instant startedAt,
        Instant deadline,
        String vmOtt,
        @Nullable String vmSubjectId,
        @Nullable String tunnelPodName,
        @Nullable Map<String, String> allocatorMeta,
        @Nullable List<VolumeClaim> volumeClaims
    ) {
        public AllocateState(String operationId, Instant startedAt, Instant deadline, String vmOtt) {
            this(operationId, startedAt, deadline, vmOtt, null, null, null, null);
        }

        public AllocateState withVmSubjId(String vmSubjId) {
            return new AllocateState(operationId, startedAt, deadline, vmOtt, vmSubjId, tunnelPodName, allocatorMeta,
                volumeClaims);
        }

        public AllocateState withTunnelPod(String tunnelPod) {
            return new AllocateState(operationId, startedAt, deadline, vmOtt, vmSubjectId, tunnelPod, allocatorMeta,
                volumeClaims);
        }

        public AllocateState withAllocatorMeta(Map<String, String> allocatorMeta) {
            return new AllocateState(operationId, startedAt, deadline, vmOtt, vmSubjectId, tunnelPodName, allocatorMeta,
                volumeClaims);
        }

        public AllocateState withVolumeClaims(List<VolumeClaim> volumeClaims) {
            return new AllocateState(operationId, startedAt, deadline, vmOtt, vmSubjectId, tunnelPodName, allocatorMeta,
                volumeClaims);
        }
    }

    public record RunState(
        @Nullable LinkedHashMap<String, String> vmMeta,
        @Nullable Instant lastActivityTime,
        @Nullable Instant deadline
    ) {}

    public enum Status {
        // Vm spec saved, but allocation not started yet (internal state, not visible for user)
        INIT,
        // Vm is allocating
        ALLOCATING,
        // Vm is running and client is holding control of it
        RUNNING,
        // Vm is running, but not allocated for client
        IDLE,
        // VM is going to be deleted (session removed)
        DELETING,
        DEAD
    }

    public String vmId() {
        return spec.vmId;
    }

    public String sessionId() {
        return spec.sessionId;
    }

    public String poolLabel() {
        return spec.poolLabel;
    }

    public String zone() {
        return spec.zone;
    }

    public List<Workload> initWorkloads() {
        return spec.initWorkloads;
    }

    public List<Workload> workloads() {
        return spec.workloads;
    }

    public List<VolumeRequest> volumeRequests() {
        return spec.volumeRequests;
    }

    @Nullable
    public Inet6Address proxyV6Address() {
        return spec.proxyV6Address;
    }

    public String allocOpId() {
        return allocateState.operationId;
    }

    public Vm withVmSubjId(String vmSubjId) {
        return new Vm(spec, status, allocateState.withVmSubjId(vmSubjId), runState);
    }

    public Vm withTunnelPod(String tunnelPod) {
        return new Vm(spec, status, allocateState.withTunnelPod(tunnelPod), runState);
    }

}
