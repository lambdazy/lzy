package ai.lzy.slots.transfers;

import ai.lzy.storage.StorageClientFactory;
import ai.lzy.v1.common.LC;
import jakarta.annotation.Nullable;

import java.util.function.Supplier;

public class TransferFactory {
    private final StorageClientFactory clientFactory;
    private final Supplier<String> tokenSupplier;

    public TransferFactory(StorageClientFactory clientFactory, Supplier<String> tokenSupplier) {
        this.clientFactory = clientFactory;
        this.tokenSupplier = tokenSupplier;
    }

    @Nullable
    public OutputTransfer output(LC.PeerDescription peer) {
        if (peer.hasStoragePeer()) {
            return new StorageOutputTransfer(peer, clientFactory);
        }

        return null;
    }
    public InputTransfer input(LC.PeerDescription peer, long offset) {
        if (peer.hasStoragePeer()) {
            return new StorageInputTransfer(peer, clientFactory);
        } else {
            return new SlotInputTransfer(peer, offset, tokenSupplier);
        }
    }
}
