package ru.yandex.cloud.ml.platform.lzy.model.graph;

import java.util.stream.Stream;

public interface Provisioning {
    interface Tag {
        String tag();
    }

    Stream<Tag> tags();

    class Any implements Provisioning {
        @Override
        public Stream<Tag> tags() {
            return Stream.empty();
        }
    }
}
