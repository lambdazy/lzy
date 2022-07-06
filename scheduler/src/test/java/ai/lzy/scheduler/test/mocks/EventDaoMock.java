package ai.lzy.scheduler.test.mocks;

import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.models.ServantEvent;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EventDaoMock implements ServantEventDao {
    private final Map<String, ServantEvent> events = new ConcurrentHashMap<>();

    @Override
    public void save(ServantEvent event) {
        events.put(event.id(), event);
    }

    @Override
    public synchronized ServantEvent take(String servantId) {
        var event = events.values()
            .stream()
            .filter(e -> e.timestamp().isBefore(Instant.now()))
            .filter(t -> Objects.equals(t.servantId(), servantId))
            .min(Comparator.naturalOrder())
            .orElse(null);
        if (event != null) {
            events.remove(event.id());
        }
        return event;
    }

    @Override
    public void removeAllByTypes(String servantId, ServantEvent.Type... types) {
        Set<ServantEvent.Type> typeSet = new HashSet<>(Arrays.asList(types));
        events.values()
            .stream()
            .filter(e -> e.servantId().equals(servantId) && typeSet.contains(e.type()))
            .map(ServantEvent::id)
            .forEach(events::remove);
    }

    @Override
    public void removeAll(String servantId) {
        events.values()
            .stream()
            .filter(e -> e.servantId().equals(servantId))
            .map(ServantEvent::id)
            .forEach(events::remove);
    }
}
