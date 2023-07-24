package ai.lzy.allocator.alloc.impl.kuber;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
@Requires(property = "allocator.policy.enabled", value = "false", defaultValue = "false")
public class DummyNetworkPolicyManager implements NetworkPolicyManager {

    @Override
    public void createNetworkPolicy(String sessionId, List<PolicyRule> whitelistCIDRs) {
    }
    @Override
    public void deleteNetworkPolicy(String sessionId) {
    }
}
