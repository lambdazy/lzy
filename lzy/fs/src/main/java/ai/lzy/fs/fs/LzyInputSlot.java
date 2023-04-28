package ai.lzy.fs.fs;

import com.google.protobuf.ByteString;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.stream.Stream;

public interface LzyInputSlot extends LzySlot {
    void connect(URI slotUri, Stream<ByteString> dataProvider);
    void disconnect();
    void destroy();

    @Nullable
    URI connectedTo();
}
