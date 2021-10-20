package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import com.google.protobuf.ByteString;

import java.net.URI;

public interface LzyInputSlot extends LzySlot {
    void connect(URI slotUri);
    void disconnect();
    void destroy();

    long writeChunk(ByteString chunk);
    void writeFinished();
}
