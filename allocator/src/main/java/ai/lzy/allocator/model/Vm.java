package ai.lzy.allocator.model;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record Vm(
    String sessionId,
    String vmId,
    String poolLabel,
    String zone,
    State state,
    String allocationOperationId,
    List<Workload> workloads,

    @Nullable Instant lastActivityTime,
    @Nullable Instant deadline,
    @Nullable Instant allocationDeadline,
    Map<String, String> vmMeta
) {
    @Override
    public String toString() {
        return "<"
            + "sessionId='"
            + sessionId + '\''
            + ", vmId='" + vmId + '\''
            + ", poolLabel='" + poolLabel + '\''
            + ", state=" + state
            + '>';
    }

    public enum State {
        // Vm created, but allocation not started yet
        CREATED,
        // Vm is allocating
        CONNECTING,
        // Vm is running, but not allocated for client
        IDLE,
        // Vm is running and client is holding control of it
        RUNNING,
        DEAD
    }

    public static class VmBuilder {
        private final String sessionId;
        private final String vmId;
        private final String poolLabel;
        private final String zone;
        private final String opId;
        private final List<Workload> workload;
        private final Map<String, String> vmMeta;
        private State state;
        private Instant lastActivityTime;
        private Instant deadline;
        private Instant allocationDeadline;

        public VmBuilder(String sessionId, String vmId, String poolLabel, String zone, String opId,
                         List<Workload> workload, State state) {
            this.sessionId = sessionId;
            this.vmId = vmId;
            this.poolLabel = poolLabel;
            this.zone = zone;
            this.opId = opId;
            this.workload = workload;
            this.state = state;
            this.vmMeta = new HashMap<>();
        }

        public VmBuilder(Vm vm) {
            this.sessionId = vm.sessionId();
            this.vmId = vm.vmId();
            this.poolLabel = vm.poolLabel();
            this.zone = vm.zone();
            this.opId = vm.allocationOperationId();
            this.workload = vm.workloads();
            this.state = vm.state();
            this.lastActivityTime = vm.lastActivityTime();
            this.deadline = vm.deadline();
            this.allocationDeadline = vm.allocationDeadline();
            this.vmMeta = vm.vmMeta();
        }

        public VmBuilder setState(State state) {
            this.state = state;
            return this;
        }

        public VmBuilder setLastActivityTime(Instant lastActivityTime) {
            this.lastActivityTime = lastActivityTime;
            return this;
        }

        public VmBuilder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public VmBuilder setAllocationDeadline(Instant allocationDeadline) {
            this.allocationDeadline = allocationDeadline;
            return this;
        }

        public VmBuilder setVmMeta(Map<String, String> vmMeta) {
            this.vmMeta.clear();
            this.vmMeta.putAll(vmMeta);
            return this;
        }

        public Vm build() {
            return new Vm(sessionId, vmId, poolLabel, zone, state, opId, workload, lastActivityTime,
                    deadline, allocationDeadline, vmMeta);
        }
    }
}
