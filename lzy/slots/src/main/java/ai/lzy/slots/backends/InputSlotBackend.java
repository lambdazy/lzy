package ai.lzy.slots.backends;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

public interface InputSlotBackend {
    SeekableByteChannel openChannel() throws IOException;

    // Closes input slot and returns output slot
    OutputSlotBackend toOutput();

    void close() throws IOException;
}
