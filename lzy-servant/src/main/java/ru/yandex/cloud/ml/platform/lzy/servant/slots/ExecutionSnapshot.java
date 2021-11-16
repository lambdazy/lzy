package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public interface ExecutionSnapshot {
    // save to persistent storage slot data
    void onChunkInput(ByteString chunk, Slot slot);

    // save to persistent storage sender slot data
    void onChunkOutput(ByteString chunk, Slot slot);
}
