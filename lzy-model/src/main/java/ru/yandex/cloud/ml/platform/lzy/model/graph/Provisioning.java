package ru.yandex.cloud.ml.platform.lzy.model.graph;

import java.util.stream.Stream;

public interface Provisioning {
    Stream<Tag> tags();

    interface Tag {
        String tag();
    }

    class Any implements Provisioning {
        @Override
        public Stream<Tag> tags() {
            return Stream.empty();
        }
    }
}
