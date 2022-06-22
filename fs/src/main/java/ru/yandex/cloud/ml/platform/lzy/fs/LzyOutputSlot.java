package ru.yandex.cloud.ml.platform.lzy.fs;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.stream.Stream;

public interface LzyOutputSlot extends LzySlot {
    Stream<ByteString> readFromPosition(long offset) throws IOException;
}
