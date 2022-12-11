package ai.lzy.allocator.vmpool;

import ai.lzy.v1.VmPoolServiceApi.GetVmPoolsRequest;
import ai.lzy.v1.VmPoolServiceApi.VmPoolSpec;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class VmPoolClient {
    private static final Logger LOG = LogManager.getLogger(VmPoolClient.class);

    public static Set<String> findZones(Collection<String> requiredPoolLabels, VmPoolServiceBlockingStub grpcClient) {
        if (requiredPoolLabels.isEmpty()) {
            LOG.warn("Collection with required pool labels is empty");
            return Collections.emptySet();
        }

        var allUserPools = grpcClient.getVmPools(
            GetVmPoolsRequest.newBuilder()
                .setWithSystemPools(false)
                .setWithUserPools(true)
                .build())
            .getUserPoolsList();

        var requiredPools = new ArrayList<VmPoolSpec>(requiredPoolLabels.size());
        for (var label : requiredPoolLabels) {
            var found = allUserPools.stream()
                .filter(pool -> label.equals(pool.getLabel()))
                .findAny();

            if (found.isEmpty()) {
                LOG.error("Cannot find pool with label: " + label);
                return Collections.emptySet();
            }

            requiredPools.add(found.get());
        }

        return requiredPools.stream()
            .collect(
                () -> new HashSet<>(requiredPools.get(0).getZonesList()),
                (common, spec) -> common.retainAll(spec.getZonesList()),
                Collection::addAll);
    }
}
