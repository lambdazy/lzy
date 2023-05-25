package ai.lzy.channelmanager.v2;

import jakarta.inject.Singleton;

@Singleton
public class ConnectAction {
    private final PeerDao peerDao;

    public ConnectAction(PeerDao peerDao) {
        this.peerDao = peerDao;
    }
}
