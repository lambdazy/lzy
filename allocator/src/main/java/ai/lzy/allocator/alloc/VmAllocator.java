package ai.lzy.allocator.alloc;

import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse;

import javax.annotation.Nullable;
import java.util.List;

public interface VmAllocator {
    /**
     * Start vm allocation
     *
     * @param vmSpec specify parameters for vm allocation
     */
    void allocate(Vm.Spec vmSpec) throws InvalidConfigurationException;

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
