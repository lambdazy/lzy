package ai.lzy.allocator.networkpolicy;

public interface NetworkPolicyManager {
    void createNetworkPolicy(String sessionId);
    void deleteNetworkPolicy(String sessionId);
}
