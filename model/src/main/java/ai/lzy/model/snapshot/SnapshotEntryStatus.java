package ai.lzy.model.snapshot;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import ai.lzy.model.data.DataSchema;

public interface SnapshotEntryStatus {

    boolean empty();

    State status();

    @Nullable
    URI storage();

    SnapshotEntry entry();

    Set<String> dependentEntryIds();

    @Nullable
    DataSchema schema();

    // CREATED --> created but not assigned to storage and slots
    // IN_PROGRESS --> started saving data
    // FINISHED --> finished saving data
    enum State {
        CREATED,
        IN_PROGRESS,
        FINISHED,
        ERRORED
    }

    class Impl implements SnapshotEntryStatus {

        private final boolean empty;
        private final State status;
        private final SnapshotEntry entry;
        private final Set<String> deps;
        private final URI storage;
        private final DataSchema schema;

        public Impl(boolean empty, State status, SnapshotEntry entry, Set<String> deps, URI storage,
            DataSchema schema) {
            this.empty = empty;
            this.status = status;
            this.entry = entry;
            this.deps = new HashSet<>(deps);
            this.storage = storage;
            this.schema = schema;
        }

        public boolean empty() {
            return empty;
        }

        public State status() {
            return status;
        }

        @Nullable
        @Override
        public URI storage() {
            return storage;
        }

        public SnapshotEntry entry() {
            return entry;
        }

        public Set<String> dependentEntryIds() {
            return deps;
        }

        @Nullable
        @Override
        public DataSchema schema() {
            return schema;
        }

        public String toString() {
            return "snapshot entry: {" + entry + "}, state: " + status + ", storage uri: " + storage + ", empty: "
                + empty + ", dependent entry ids: {" + Arrays.toString(deps.toArray()) + "}";
        }
    }
}