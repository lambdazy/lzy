package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import java.net.URI;
import java.nio.file.Path;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshotProvider;

public class LocalOutFileSlot extends OutFileSlot {
    private final Path location;

    public LocalOutFileSlot(String tid, Slot definition, URI uri, SlotSnapshotProvider snapshotProvider) {
        super(tid, definition, Path.of(uri.getPath()), snapshotProvider);
        location = Path.of(uri.getPath());
    }

    @Override
    public Path location() {
        return location;
    }

    @Override
    public String toString() {
        return "LocalOutFileSlot:" + definition().name() + "->" + location().toString();
    }
}
