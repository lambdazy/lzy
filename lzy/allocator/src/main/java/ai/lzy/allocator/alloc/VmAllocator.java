package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;

import java.util.List;
import javax.annotation.Nullable;

public interface VmAllocator {

    class AllocateResult {
        public enum Code {
            SUCCESS,
            RETRY_LATER,
            FAILED
        }

        private final Code code;
        private final String reason;

        public static final AllocateResult SUCCESS = new AllocateResult(Code.SUCCESS, "");
        public static final AllocateResult RETRY_LATER = new AllocateResult(Code.RETRY_LATER, "");
        public static final AllocateResult FAILED = new AllocateResult(Code.FAILED, "");

        private AllocateResult(Code code, String reason) {
            this.code = code;
            this.reason = reason;
        }

        public Code code() {
            return code;
        }

        public String message() {
            return reason;
        }

        public AllocateResult withReason(String reason) {
            return new AllocateResult(code, reason);
        }
    }

    /**
     * Start vm allocation.
     *
     * @throws InvalidConfigurationException on invalid spec
     */
    AllocateResult allocate(Vm vm) throws InvalidConfigurationException;

    /**
     * Idempotent operation to destroy vm
     * If vm is not allocated, does nothing
     *
     * @param vmId of vm to deallocate
     */
    void deallocate(String vmId);

    /**
     * Get endpoints of vm to connect to it
     * @param vmId id of vm to get hosts
     * @return list of vm's endpoints
     */
    List<VmEndpoint> getVmEndpoints(String vmId, @Nullable TransactionHandle transaction);

    record VmEndpoint(
        VmEndpointType type,
        String value
    ) {

        public AllocateResponse.VmEndpoint toProto() {
            final var typ = switch (type) {
                case HOST_NAME -> AllocateResponse.VmEndpoint.VmEndpointType.HOST_NAME;
                case EXTERNAL_IP -> AllocateResponse.VmEndpoint.VmEndpointType.EXTERNAL_IP;
                case INTERNAL_IP -> AllocateResponse.VmEndpoint.VmEndpointType.INTERNAL_IP;
            };

            return AllocateResponse.VmEndpoint.newBuilder()
                .setValue(value)
                .setType(typ)
                .build();
        }
    }

    enum VmEndpointType {
        HOST_NAME,
        EXTERNAL_IP,
        INTERNAL_IP
    }
}
