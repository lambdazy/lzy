package ai.lzy.env.logs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Logs implements AutoCloseable {
    private final List<LogStream> streams = new ArrayList<>();
    private final Map<String, LogStreamQueue> queues = new HashMap<>();

    public Logs(List<LogWriter> writers, List<LogStreamCollection> collections) {
        for (var collection: collections) {
            for (var stream: collection.getStreams()) {
                // Using one queue for streams with same names
                var queue = queues.computeIfAbsent(stream.name(), k -> new LogStreamQueue(stream.name(), writers));

                queue.start();
                stream.init(queue);
                streams.add(stream);
            }
        }
    }

    @Override
    public void close() {
        for (var queue: queues.values()) {
            try {
                queue.close();
            } catch (InterruptedException e) {
                // ignored
            }
        }

        for (var stream: streams) {
            stream.await();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<LogWriter> writers = new ArrayList<>();
        private final List<LogStreamCollection> collections = new ArrayList<>();

        public Builder withWriters(LogWriter... writers) {
            this.writers.addAll(List.of(writers));
            return this;
        }

        public Builder withCollections(LogStreamCollection... collections) {
            this.collections.addAll(List.of(collections));
            return this;
        }

        public Logs build() {
            return new Logs(writers, collections);
        }
    }
}
