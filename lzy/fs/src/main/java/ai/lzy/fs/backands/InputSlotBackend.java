package ai.lzy.fs.backands;

import java.io.IOException;
import java.io.OutputStream;

public interface InputSlotBackend {
    OutputStream outputStream() throws IOException;

    // Closes input slot and returns output slot
    OutputSlotBackend toOutput();

    void close() throws IOException;
}
