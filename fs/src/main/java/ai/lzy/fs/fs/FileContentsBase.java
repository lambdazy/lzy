package ai.lzy.fs.fs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public abstract class FileContentsBase implements FileContents {
    private final AtomicReference<List<ContentsTracker>> trackers = new AtomicReference<>(List.of());

    public final void track(ContentsTracker tracker) {
        List<ContentsTracker> trackers;
        List<ContentsTracker> newValue;
        do {
            trackers = this.trackers.get();
            newValue = new ArrayList<>(trackers);
            newValue.add(tracker);
        } while (!this.trackers.compareAndSet(trackers, newValue));
    }

    protected Stream<ContentsTracker> trackers() {
        return trackers.get().stream();
    }
}
