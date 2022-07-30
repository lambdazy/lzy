package ai.lzy.fs.snapshot;

import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.net.URI;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;

public interface SlotSnapshot {
    // generate slot key
    URI uri();

    // save to persistent storage slot data
    void onChunk(ByteString chunk);

    boolean isEmpty();

    void onFinish();
}
