package ai.lzy.scheduler.test.mocks;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.impl.EventQueueManager;
import ai.lzy.scheduler.servant.impl.ServantImpl;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ServantDaoMock implements ServantDao {
    private final Map<ServantKey, ServantDesc> storage = new ConcurrentHashMap<>();
    private final EventQueueManager queueManager;

    public ServantDaoMock(EventQueueManager queueManager) {
        this.queueManager = queueManager;
    }

    @Nullable
    @Override
    public synchronized ServantState acquire(String workflowId, String servantId) throws AcquireException {
        var servant = storage.get(new ServantKey(workflowId, servantId));
        if (servant == null) {
            return null;
        }
        if (servant.acquired()) {
            throw new AcquireException();
        }
        storage.put(new ServantKey(workflowId, servantId),
                new ServantDesc(servant.state(), true, servant.acquiredForTask()));
        return servant.state();
    }

    @Override
    public void updateAndFree(ServantState resource) {
        var key = new ServantKey(resource.workflowId(), resource.id());
        var servant = storage.get(key);
        storage.put(new ServantKey(resource.workflowId(), resource.id()),
                new ServantDesc(resource, false, servant.acquiredForTask()));
    }

    @Override
    public List<Servant> getAllFree() {
        return storage.values()
            .stream()
            .filter(t -> t.state().status() != ServantState.Status.DESTROYED && !t.acquired())
            .map(t -> (Servant) new ServantImpl(t.state(), queueManager.get(t.state().workflowId(), t.state.id())))
            .toList();
    }

    @Override
    public List<Servant> getAllAcquired() {
        return storage.values()
            .stream()
            .filter(t -> t.state().status() != ServantState.Status.DESTROYED && t.acquired())
            .map(t -> (Servant) new ServantImpl(t.state(), queueManager.get(t.state().workflowId(), t.state.id())))
            .toList();
    }

    @Override
    public Servant create(String workflowId, Provisioning provisioning) {
        var servantId = UUID.randomUUID().toString();
        var state = new ServantState.ServantStateBuilder(servantId, workflowId,
            provisioning, ServantState.Status.CREATED).build();
        storage.put(new ServantKey(workflowId, servantId), new ServantDesc(state, false, false));
        return new ServantImpl(state, queueManager.get(state.workflowId(), state.id()));
    }

    @Override
    public int countAlive(String workflowId, Provisioning provisioning) {
        return (int) storage.values()
            .stream()
            .filter(t -> t.state().workflowId().equals(workflowId) && contains(t.state().provisioning(), provisioning))
            .count();
    }

    @Nullable
    @Override
    public Servant get(String workflowId, String servantId) {
        var servant = storage.get(new ServantKey(workflowId, servantId));
        if (servant == null) {
            return null;
        }
        return new ServantImpl(servant.state(), queueManager.get(servant.state.workflowId(), servant.state.id()));
    }

    @Nullable
    @Override
    public synchronized Servant acquireForTask(String workflowId,
                                               Provisioning provisioning, ServantState.Status... statuses) {
        var statusSet = new HashSet<>(Arrays.asList(statuses));
        var servant = storage.values()
            .stream()
            .filter(t -> t.state.workflowId().equals(workflowId)
                && contains(t.state.provisioning(), provisioning)
                && statusSet.contains(t.state().status())
                && !t.acquiredForTask()
            )
            .findFirst()
            .orElse(null);
        if (servant == null) {
            return null;
        }
        storage.put(new ServantKey(workflowId, servant.state.id()),
                new ServantDesc(servant.state(), servant.acquired(), true));
        return new ServantImpl(servant.state, queueManager.get(servant.state.workflowId(), servant.state.id()));
    }

    @Override
    public void freeFromTask(String workflowId, String servantId) {
        var servant = storage.get(new ServantKey(workflowId, servantId));
        if (servant == null) {
            return;
        }
        storage.put(new ServantKey(workflowId, servantId),
                new ServantDesc(servant.state, servant.acquired, false));
    }

    @Override
    public synchronized void invalidate(Servant servant, String description) {
        var state = new ServantState.ServantStateBuilder(servant.id(), servant.workflowId(),
                servant.provisioning(), ServantState.Status.DESTROYED)
                .setErrorDescription(description)
                .build();
        storage.put(new ServantKey(servant.workflowId(), servant.id()),
                new ServantDesc(state, false, false));
    }

    @Override
    public List<Servant> get(String workflowId) throws DaoException {
        return storage.values().stream()
            .filter(t -> t.state().workflowId().equals(workflowId))
            .map(s -> (Servant) new ServantImpl(s.state(), queueManager.get(s.state.workflowId(), s.state.id())))
            .toList();
    }

    private record ServantKey(String workflowId, String servantId) {}
    private record ServantDesc(ServantState state, boolean acquired, boolean acquiredForTask) {}

    private boolean contains(Provisioning p1, Provisioning p2) {
        return p1.tags().collect(Collectors.toSet()).containsAll(p2.tags().toList());
    }

    public void awaitState(String workflowId, String servantId,
                           ServantState.Status status) throws InterruptedException {
        var key = new ServantKey(workflowId, servantId);
        ServantState.Status s = null;
        var servant = storage.get(key);
        if (servant != null) {
            s = servant.state().status();
        }
        while (s == null || s != status) {
            Thread.sleep(10);
            servant = storage.get(key);
            if (servant == null) {
                s = null;
            } else {
                s = servant.state().status();
            }
        }
    }
}
