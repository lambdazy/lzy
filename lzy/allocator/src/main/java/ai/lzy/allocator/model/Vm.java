package ai.lzy.allocator.model;

import ai.lzy.allocator.vmpool.ClusterRegistry;
import jakarta.annotation.Nullable;

import java.net.Inet6Address;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record Vm(
    Spec spec,
    Status status,
    InstanceProperties instanceProperties,
    AllocateState allocateState,
    @Nullable RunState runState,
    @Nullable IdleState idleState,
    @Nullable DeletingState deleteState
) {
    public Vm(Spec spec, Status status, AllocateState allocateState) {
        this(spec, status, new InstanceProperties(null, null), allocateState, null, null, null);
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
        @Nullable Inet6Address proxyV6Address,
        ClusterRegistry.ClusterType clusterType
    ) {
        public Spec withVmId(String vmId) {
            return new Spec(vmId, sessionId, poolLabel, zone, initWorkloads,
                workloads, volumeRequests, proxyV6Address, clusterType);
        }
    }

    public record InstanceProperties(
        @Nullable String vmSubjectId,
        @Nullable String tunnelPodName
    ) {
        public InstanceProperties withVmSubjId(String vmSubjectId) {
            return new InstanceProperties(vmSubjectId, tunnelPodName);
        }

        public InstanceProperties withTunnelPod(String tunnelPodName) {
            return new InstanceProperties(vmSubjectId, tunnelPodName);
        }
    }

    public record AllocateState(
        String operationId,
        Instant startedAt,
        Instant deadline,
        String worker,
        String reqid,
        String vmOtt,
        @Nullable Map<String, String> allocatorMeta,
        @Nullable List<VolumeClaim> volumeClaims
    ) {
        public AllocateState(String operationId, Instant startedAt, Instant deadline, String worker, String reqid,
                             String vmOtt)
        {
            this(operationId, startedAt, deadline, worker, reqid, vmOtt, null, null);
        }

        public AllocateState withAllocatorMeta(Map<String, String> allocatorMeta) {
            return new AllocateState(operationId, startedAt, deadline, worker, reqid, vmOtt, allocatorMeta,
                volumeClaims);
        }

        public AllocateState withVolumeClaims(List<VolumeClaim> volumeClaims) {
            return new AllocateState(operationId, startedAt, deadline, worker, reqid, vmOtt, allocatorMeta,
                volumeClaims);
        }
    }

    public record RunState(
        LinkedHashMap<String, String> vmMeta,
        Instant activityDeadline
    ) {}

    public record IdleState(
        Instant idleSice,
        Instant deadline
    ) {}

    public record DeletingState(
        String operationId,
        String worker,
        String reqid
    ) {}

    public enum Status {
        // Vm is allocating
        ALLOCATING,
        // Vm is running and client is holding control of it
        RUNNING,
        // Vm is running, but not allocated for client
        IDLE,
        // VM is going to be deleted (i.e. session removed or allocation was failed/cancelled)
        DELETING
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
        return new Vm(spec, status, instanceProperties.withVmSubjId(vmSubjId), allocateState, runState, idleState,
            deleteState);
    }

    public Vm withTunnelPod(String tunnelPod) {
        return new Vm(spec, status, instanceProperties.withTunnelPod(tunnelPod), allocateState, runState, idleState,
            deleteState);
    }

    public Vm withAllocateState(AllocateState allocateState) {
        return new Vm(spec, status, instanceProperties, allocateState, runState, idleState, deleteState);
    }

    public static final class Ref {
        private volatile Vm vm;

        public Ref(Vm vm) {
            this.vm = vm;
        }

        public Vm vm() {
            return vm;
        }

        public void setVm(Vm vm) {
            this.vm = vm;
        }
    }
}
