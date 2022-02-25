package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;
import java.util.Date;

public interface Snapshot {
    URI id();

    URI uid();

    Date creationDateUTC();

    String workflowName();

    class Impl implements Snapshot {
        private final URI id;
        private final URI uid;
        private final Date creationDateUTC;
        private final String workflowName;

        public Impl(URI id, URI uid, Date creationDateUTC, String workflowName) {
            this.id = id;
            this.uid = uid;
            this.creationDateUTC = creationDateUTC;
            this.workflowName = workflowName;
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
    }
}
