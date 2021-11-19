package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.google.protobuf.ByteString;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;

import java.net.URI;

public interface ExecutionSnapshot {
    // generate slot key
    URI getSlotUri(Slot slot);

    // save to persistent storage slot data
    void onChunkInput(ByteString chunk, Slot slot);

    // save to persistent storage sender slot data
    void onChunkOutput(ByteString chunk, Slot slot);

    void onFinish(Slot slot);
}
