package ai.lzy.allocator.vmpool;

import ai.lzy.v1.VmPoolServiceApi.GetVmPoolsRequest;
import ai.lzy.v1.VmPoolServiceApi.VmPoolSpec;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import javax.annotation.Nullable;

public class VmPoolClient {
    private static final Logger LOG = LogManager.getLogger(VmPoolClient.class);

    @Nullable
    public static String findZone(VmPoolServiceBlockingStub vmPoolClient, Collection<String> requiredPoolLabels) {
        List<VmPoolSpec> availablePools = vmPoolClient.getVmPools(GetVmPoolsRequest.newBuilder()
            .setWithSystemPools(false)
            .setWithUserPools(true)
            .build()).getUserPoolsList();

        var requiredPools = new ArrayList<VmPoolSpec>(requiredPoolLabels.size());
        for (var targetLabel : requiredPoolLabels) {
            var found = availablePools.parallelStream()
                .filter(pool -> targetLabel.contentEquals(pool.getLabel())).findAny();
            if (found.isEmpty()) {
                LOG.error("Cannot find pool with label: " + targetLabel);
                return null;
            }
            requiredPools.add(found.get());
        }

        Set<String> commonZones = requiredPools.stream().collect(
            () -> new HashSet<>(requiredPools.get(0).getZonesList()),
            (common, spec) -> common.retainAll(spec.getZonesList()), Collection::addAll);

        return commonZones.stream().findAny().orElseGet(() -> {
            LOG.error("Cannot find zone which has all required pools");
            return null;
        });
    }
}
