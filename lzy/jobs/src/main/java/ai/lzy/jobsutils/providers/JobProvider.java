package ai.lzy.jobsutils.providers;

import javax.annotation.Nullable;

public interface JobProvider {
    void execute(@Nullable Object arg);
}
