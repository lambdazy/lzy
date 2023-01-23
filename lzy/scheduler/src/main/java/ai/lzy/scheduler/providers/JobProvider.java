package ai.lzy.scheduler.providers;

import javax.annotation.Nullable;

public interface JobProvider {
    void execute(@Nullable Object arg);
}
