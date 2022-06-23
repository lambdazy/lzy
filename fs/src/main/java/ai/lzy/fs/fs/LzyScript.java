package ai.lzy.fs.fs;

import ai.lzy.model.Zygote;

import java.nio.file.Path;

public interface LzyScript {
    Zygote operation();

    Path location();

    CharSequence scriptText();
}
