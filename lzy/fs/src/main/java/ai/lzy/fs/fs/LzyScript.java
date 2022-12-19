package ai.lzy.fs.fs;

import java.nio.file.Path;

public interface LzyScript {
    Path location();

    CharSequence scriptText();
}
