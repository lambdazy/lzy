package ru.yandex.cloud.ml.platform.lzy.model.snapshot;

import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public interface Whiteboard {
    URI id();

    Set<String> fieldNames();

    Set<String> tags();

    Snapshot snapshot();

    String namespace();

    Date creationDateUTC();

    class Impl implements Whiteboard {
        private final URI id;
        private final Set<String> fieldNames;
        private final Set<String> tags;
        private final Snapshot snapshot;
        private final String namespace;
        private final Date creationDateUTC;

        public Impl(URI id, Set<String> fieldNames, Snapshot snapshot, Set<String> tags,
            String namespace, Date creationDateUTC) {
            this.id = id;
            this.fieldNames = new HashSet<>(fieldNames);
            this.tags = new HashSet<>(tags);
            this.snapshot = snapshot;
            this.namespace = namespace;
            this.creationDateUTC = creationDateUTC;
        }

        @Override
        public URI id() {
            return id;
        }

        @Override
        public Set<String> fieldNames() {
            return new HashSet<>(fieldNames);
        }

        @Override
        public Set<String> tags() {
            return tags;
        }

        @Override
        public Snapshot snapshot() {
            return snapshot;
        }

        @Override
        public String namespace() {
            return namespace;
        }

        @Override
        public Date creationDateUTC() {
            return creationDateUTC;
        }
    }
}
