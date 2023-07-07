package ai.lzy.allocator.alloc.impl.kuber;

import java.util.List;

public interface NetworkPolicyManager {
    void createNetworkPolicy(String sessionId, List<PolicyRule> whitelistCIDRs);

    void deleteNetworkPolicy(String sessionId);

    record PolicyRule(String cidr, List<Integer> ports) {}
}
