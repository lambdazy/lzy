package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;
import java.util.Date;
import javax.annotation.Nullable;

public interface Snapshot {
    URI id();

    URI uid();

    Date creationDateUTC();

    String workflowName();

    @Nullable
    String parentSnapshotId();

    class Impl implements Snapshot {
        private final URI id;
        private final URI uid;
        private final Date creationDateUTC;
        private final String workflowName;
        @Nullable private final String parentSnapshotId;

        public Impl(URI id, URI uid, Date creationDateUTC, String workflowName, @Nullable String parentSnapshotId) {
            this.id = id;
            this.uid = uid;
            this.creationDateUTC = creationDateUTC;
            this.workflowName = workflowName;
            this.parentSnapshotId = parentSnapshotId;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public URI uid() {
            return uid;
        }

        @Override
        public Date creationDateUTC() {
            return creationDateUTC;
        }

        @Override
        public String workflowName() {
            return workflowName;
        }

        @Nullable
        @Override
        public String parentSnapshotId() {
            return parentSnapshotId;
        }
    }
}
