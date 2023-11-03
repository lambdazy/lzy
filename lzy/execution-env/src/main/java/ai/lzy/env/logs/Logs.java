package ai.lzy.env.logs;

import java.util.ArrayList;
import java.util.List;

public class Logs implements AutoCloseable {
    private final List<LogStream> streams = new ArrayList<>();
    private final List<LogStreamQueue> queues = new ArrayList<>();

    public Logs(List<LogWriter> writers, List<LogStreamCollection> collections) {
        for (var collection: collections) {
            for (var stream: collection.getStreams()) {
                var queue = new LogStreamQueue(stream.name(), writers);
                stream.init(queue);
                streams.add(stream);
                queues.add(queue);
            }
        }
    }

    @Override
    public void close() {
        for (var queue: queues) {
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
