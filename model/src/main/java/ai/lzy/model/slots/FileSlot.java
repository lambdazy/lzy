package ai.lzy.model.slots;


import java.nio.file.Path;
import ai.lzy.model.Slot;

public interface FileSlot extends Slot {
    Path mount();

    @Override
    default Media media() {
        return Media.FILE;
    }
}
