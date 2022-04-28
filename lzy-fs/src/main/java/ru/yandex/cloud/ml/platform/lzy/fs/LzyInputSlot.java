package ru.yandex.cloud.ml.platform.lzy.fs;

import com.google.protobuf.ByteString;

import java.net.URI;
import java.util.stream.Stream;

public interface LzyInputSlot extends LzySlot {
    void connect(URI slotUri, Stream<ByteString> dataProvider);
    void disconnect();
    void destroy();
}
