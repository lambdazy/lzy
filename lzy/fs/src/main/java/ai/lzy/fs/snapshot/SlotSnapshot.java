package ai.lzy.fs.snapshot;

import com.google.protobuf.ByteString;

import java.net.URI;

public interface SlotSnapshot {
    // generate slot key
    URI uri();

    // save to persistent storage slot data
    void onChunk(ByteString chunk);

    boolean isEmpty();

    void onFinish();
}
