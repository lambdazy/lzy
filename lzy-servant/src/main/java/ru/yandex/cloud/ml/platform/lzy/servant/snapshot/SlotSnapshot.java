package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.net.URI;

public interface SlotSnapshot {
    // generate slot key
    URI uri();

    // save to persistent storage slot data
    void onChunk(ByteString chunk);

    boolean isEmpty();

    void readAll(InputStream stream);

    void onFinish();
}
