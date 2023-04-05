package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.impl.kuber.NodeRemover;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Singleton
@Primary
@Requires(env = "test")
public class TestNodeRemover implements NodeRemover {

    private record Entry(
        String vmId,
        String nodeName,
        String nodeInstanceId
    ) {}

    private final Map<Entry, AtomicInteger> entries = new ConcurrentHashMap<>();

    @Override
    public void removeNode(String vmId, String nodeName, String nodeInstanceId) {
        entries.computeIfAbsent(new Entry(vmId, nodeName, nodeInstanceId), x -> new AtomicInteger(0)).getAndIncrement();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int size() {
        return entries.size();
    }

    public boolean contains(String vmId, String nodeName, String nodeInstanceId) {
        return entries.containsKey(new Entry(vmId, nodeName, nodeInstanceId));
    }

    public int count(String vmId, String nodeName, String nodeInstanceId) {
        var x = entries.get(new Entry(vmId, nodeName, nodeInstanceId));
        return x != null ? x.get() : 0;
    }

    public boolean await(Duration timeout) {
        var deadline = Instant.now().plus(timeout);
        do {
            if (!entries.isEmpty()) {
                return true;
            }
            LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
        } while (Instant.now().isBefore(deadline));
        return false;
    }

    public String toString() {
        var sb = new StringBuilder();
        sb.append("Removed Nodes:\n");
        for (var entry : entries.entrySet()) {
            sb.append("  ").append(entry.getKey()).append(" : ").append(entry.getValue().get()).append('\n');
        }
        return sb.toString();
    }
}
