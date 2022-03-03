package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.net.URI;
import java.util.function.Consumer;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

public interface SlotSnapshot {
    // generate slot key
    URI uri();

    // save to persistent storage slot data
    void onChunk(ByteString chunk);

    boolean isEmpty();

    void writeFromStream(InputStream stream);

    void onFinish();

    void readByChunks(String bucket, String key, ThrowingConsumer<ByteString> onChunk, Runnable onComplete);
}
