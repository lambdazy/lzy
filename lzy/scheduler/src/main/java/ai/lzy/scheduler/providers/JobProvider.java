package ai.lzy.scheduler.providers;

import jakarta.annotation.Nullable;

public interface JobProvider {
    void execute(@Nullable Object arg);
}
