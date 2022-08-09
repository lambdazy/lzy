package ai.lzy.allocator.model;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public record Vm(
    String sessionId,
    String vmId,
    String poolId,
    State state,
    String allocationOperationId,
    List<Workload> workloads,

    @Nullable Instant heartBeatTimeoutAt,
    @Nullable Instant expireAt,
    @Nullable Instant allocationTimeoutAt,
    @Nullable Map<String, String> allocatorMeta,
    @Nullable Map<String, String> vmMeta
) {
    @Override
    public String toString() {
        return "<"
            + "sessionId='"
            + sessionId + '\''
            + ", vmId='" + vmId + '\''
            + ", poolId='" + poolId + '\''
            + ", state=" + state
            + '>';
    }

    public enum State {
        // Vm created, but allocation not started yet
        CREATED,
        // Vm is allocating
        CONNECTING,
        // Vm is running, but not allocated for client
        IDLING,
        // Vm is running and client is holding control of it
        RUNNING,
        DEAD
    }

    public static class VmBuilder {
        private final String sessionId;
        private final String vmId;
        private final String poolId;
        private final String opId;
        private final List<Workload> workload;
        private State state;
        private Map<String, String> allocatorMeta;
        private Map<String, String> vmMeta;
        private Instant heartBeatTimeoutAt;
        private Instant expireAt;
        private Instant allocationTimeoutAt;

        public VmBuilder(String sessionId, String vmId, String poolId, String opId,
                 List<Workload> workload, State state) {
            this.sessionId = sessionId;
            this.vmId = vmId;
            this.poolId = poolId;
            this.opId = opId;
            this.workload = workload;
            this.state = state;
        }

        public VmBuilder(Vm vm) {
            this.sessionId = vm.sessionId();
            this.vmId = vm.vmId();
            this.poolId = vm.poolId();
            this.opId = vm.allocationOperationId();
            this.workload = vm.workloads();
            this.state = vm.state();
            this.allocatorMeta = vm.allocatorMeta();
            this.heartBeatTimeoutAt = vm.heartBeatTimeoutAt();
            this.expireAt = vm.expireAt();
            this.allocationTimeoutAt = vm.allocationTimeoutAt();
        }

        public VmBuilder setState(State state) {
            this.state = state;
            return this;
        }

        public VmBuilder setAllocatorMeta(Map<String, String> allocatorMeta) {
            this.allocatorMeta = allocatorMeta;
            return this;
        }

        public VmBuilder setHeartBeatTimeoutAt(Instant heartBeatTimeoutAt) {
            this.heartBeatTimeoutAt = heartBeatTimeoutAt;
            return this;
        }

        public VmBuilder setExpireAt(Instant expireAt) {
            this.expireAt = expireAt;
            return this;
        }

        public VmBuilder setAllocationTimeoutAt(Instant allocationTimeoutAt) {
            this.allocationTimeoutAt = allocationTimeoutAt;
            return this;
        }

        public VmBuilder setVmMeta(Map<String, String> vmMeta) {
            this.vmMeta = vmMeta;
            return this;
        }

        public Vm build() {
            return new Vm(sessionId, vmId, poolId, state, opId, workload, allocationTimeoutAt,
                heartBeatTimeoutAt, expireAt, allocatorMeta, vmMeta);
        }
    }
}
