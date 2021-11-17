package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.ExecutionSnapshot;

import java.net.URI;
import java.nio.file.Path;

public class LocalOutFileSlot extends OutFileSlot {
    private final Path location;

    public LocalOutFileSlot(String tid, Slot definition, URI uri, ExecutionSnapshot snapshot) {
        super(tid, definition, Path.of(uri.getPath()), snapshot);
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
