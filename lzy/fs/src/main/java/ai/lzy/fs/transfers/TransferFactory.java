package ai.lzy.fs.transfers;

import ai.lzy.storage.StorageClientFactory;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.common.LC;
import io.grpc.Status;

public class TransferFactory {
    private final StorageClientFactory clientFactory;
    private final RenewableJwt jwt;

    public TransferFactory(StorageClientFactory clientFactory, RenewableJwt jwt) {
        this.clientFactory = clientFactory;
        this.jwt = jwt;
    }

    public OutputTransfer output(LC.PeerDescription peer) {
        if (peer.hasStoragePeer()) {
            return new StorageOutputTransfer(peer, clientFactory);
        } else {
            throw Status.UNIMPLEMENTED.asRuntimeException();
        }
    }
    public InputTransfer input(LC.PeerDescription peer, int offset) {
        if (peer.hasStoragePeer()) {
            return new StorageInputTransfer(peer, clientFactory);
        } else {
            return new SlotInputTransfer(peer, offset, jwt);
        }
    }
}
