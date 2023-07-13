package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.exceptions.NetworkPolicyException;

import java.util.List;

public interface NetworkPolicyManager {
    void createNetworkPolicy(String sessionId, List<PolicyRule> whitelistCIDRs) throws NetworkPolicyException;

    void deleteNetworkPolicy(String sessionId) throws NetworkPolicyException;

    record PolicyRule(String cidr, List<Integer> ports) {}
}
