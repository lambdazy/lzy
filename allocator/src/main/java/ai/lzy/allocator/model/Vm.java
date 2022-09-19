package ai.lzy.allocator.model;

import ai.lzy.allocator.volume.VolumeClaim;
import ai.lzy.allocator.volume.VolumeRequest;

import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public record Vm(
    Spec spec,
    State state,
    String allocationOperationId
) {
    @Override
    public String toString() {
        return "<"
            + "sessionId='"
            + sessionId() + '\''
            + ", vmId='" + vmId() + '\''
            + ", poolLabel='" + poolLabel() + '\''
            + ", state=" + status()
            + '>';
    }

    public Temporal allocationStartedAt() {
        return spec.allocationStartedAt();
    }

    public List<VolumeRequest> volumeRequests() {
        return Collections.unmodifiableList(spec.volumeRequests());
    }

    public record Spec(
        String vmId,
        String sessionId,
        Instant allocationStartedAt,

        String poolLabel,
        String zone,

        List<Workload> workloads,
        List<VolumeRequest> volumeRequests
    ) {}

    public record State(
        VmStatus status,
        @Nullable Instant lastActivityTime,
        @Nullable Instant deadline,
        @Nullable Instant allocationDeadline,
        @Nullable String vmSubjectId,
        Map<String, String> vmMeta,

        List<VolumeClaim> volumeClaims
    ) {}

    public enum VmStatus {
        // Vm created, but allocation not started yet
        CREATED,
        // Vm is allocating
        CONNECTING,
        // Vm is running, but not allocated for client
        IDLE,
        // Vm is running and client is holding control of it
        RUNNING,
        // VM is going to be deleted (session removed)
        DELETING,
        DEAD
    }

    public String sessionId() {
        return spec.sessionId();
    }

    public String vmId() {
        return spec.vmId();
    }

    public String poolLabel() {
        return spec.poolLabel();
    }

    public String zone() {
        return spec.zone();
    }

    public List<Workload> workloads() {
        return spec.workloads();
    }

    public List<VolumeClaim> volumeClaims() {
        return state.volumeClaims();
    }

    public VmStatus status() {
        return state.status();
    }

    public Map<String, String> vmMeta() {
        return state.vmMeta();
    }

    public static class VmStateBuilder {
        private VmStatus vmStatus;
        private Instant lastActivityTime;
        private Instant deadline;
        private Instant allocationDeadline;
        private String vmSubjectId;
        private Map<String, String> vmMeta;
        private List<VolumeClaim> volumeClaims;

        public VmStateBuilder() {}

        public VmStateBuilder(State existingState) {
            this.vmStatus = existingState.status;
            if (existingState.vmMeta != null) {
                this.vmMeta = new HashMap<>();
                this.vmMeta.putAll(existingState.vmMeta);
            }
            this.lastActivityTime = existingState.lastActivityTime;
            this.deadline = existingState.deadline;
            this.allocationDeadline = existingState.allocationDeadline;
            this.vmSubjectId = existingState.vmSubjectId;
            if (existingState.volumeClaims != null) {
                this.volumeClaims = new ArrayList<>();
                this.volumeClaims.addAll(existingState.volumeClaims);
            }
        }

        public VmStateBuilder setStatus(VmStatus vmStatus) {
            this.vmStatus = vmStatus;
            return this;
        }

        public VmStateBuilder setLastActivityTime(Instant lastActivityTime) {
            this.lastActivityTime = lastActivityTime;
            return this;
        }

        public VmStateBuilder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public VmStateBuilder setAllocationDeadline(Instant allocationDeadline) {
            this.allocationDeadline = allocationDeadline;
            return this;
        }

        public VmStateBuilder setVmMeta(Map<String, String> vmMeta) {
            this.vmMeta = new HashMap<>();
            this.vmMeta.putAll(vmMeta);
            return this;
        }

        public VmStateBuilder setVolumeClaims(List<VolumeClaim> volumeClaims) {
            this.volumeClaims = new ArrayList<>();
            this.volumeClaims.addAll(volumeClaims);
            return this;
        }

        public VmStateBuilder setVmSubjectId(String vmSubjectId) {
            this.vmSubjectId = vmSubjectId;
            return this;
        }

        public State build() {
            return new State(vmStatus, lastActivityTime, deadline, allocationDeadline, vmSubjectId, vmMeta,
                volumeClaims);
        }
    }
}
