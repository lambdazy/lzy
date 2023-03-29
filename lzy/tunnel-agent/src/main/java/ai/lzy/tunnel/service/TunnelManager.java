package ai.lzy.tunnel.service;

public interface TunnelManager {
    void createTunnel(String remoteV6Address, String podV4Address, String podsCIDR, int tunnelIndex);
    void destroyTunnel();
}
