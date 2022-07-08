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

public class ServantDaoMock implements ServantDao {
    private final Map<ServantKey, ServantDesc> storage = new ConcurrentHashMap<>();
    private final EventQueueManager queueManager;

    public ServantDaoMock(EventQueueManager queueManager) {
        this.queueManager = queueManager;
    }

    @Nullable
    @Override
    public synchronized ServantState acquire(String workflowName, String servantId) throws AcquireException {
        var servant = storage.get(new ServantKey(workflowName, servantId));
        if (servant == null) {
            return null;
        }
        if (servant.acquired()) {
            throw new AcquireException();
        }
        storage.put(new ServantKey(workflowName, servantId),
                new ServantDesc(servant.state(), true, servant.acquiredForTask()));
        return servant.state();
    }

    @Override
    public void updateAndFree(ServantState resource) {
        var key = new ServantKey(resource.workflowName(), resource.id());
        var servant = storage.get(key);
        storage.put(new ServantKey(resource.workflowName(), resource.id()),
                new ServantDesc(resource, false, servant.acquiredForTask()));
    }

    @Override
    public List<Servant> getAllFree() {
        return storage.values()
            .stream()
            .filter(t -> t.state().status() != ServantState.Status.DESTROYED && !t.acquired())
            .map(t -> (Servant) new ServantImpl(t.state(), queueManager.get(t.state().workflowName(), t.state.id())))
            .toList();
    }

    @Override
    public List<Servant> getAllAcquired() {
        return storage.values()
            .stream()
            .filter(t -> t.state().status() != ServantState.Status.DESTROYED && t.acquired())
            .map(t -> (Servant) new ServantImpl(t.state(), queueManager.get(t.state().workflowName(), t.state.id())))
            .toList();
    }

    @Override
    public Servant create(String workflowName, Provisioning provisioning) {
        var servantId = UUID.randomUUID().toString();
        var state = new ServantState.ServantStateBuilder(servantId, workflowName,
            provisioning, ServantState.Status.CREATED).build();
        storage.put(new ServantKey(workflowName, servantId), new ServantDesc(state, false, false));
        return new ServantImpl(state, queueManager.get(state.workflowName(), state.id()));
    }

    @Override
    public int countAlive(String workflowName, Provisioning provisioning) {
        return (int) storage.values()
            .stream()
            .filter(t -> t.state().workflowName().equals(workflowName) && contains(t.state().provisioning(), provisioning))
            .count();
    }

    @Nullable
    @Override
    public Servant get(String workflowName, String servantId) {
        var servant = storage.get(new ServantKey(workflowName, servantId));
        if (servant == null) {
            return null;
        }
        return new ServantImpl(servant.state(), queueManager.get(servant.state.workflowName(), servant.state.id()));
    }

    @Override
    public synchronized void acquireForTask(String workflowName, String servantId) throws AcquireException {
        var servant = storage.get(new ServantKey(workflowName, servantId));
        if (servant == null) {
            return;
        }
        if (servant.acquiredForTask) {
            throw new AcquireException();
        }

        storage.put(new ServantKey(workflowName, servant.state.id()),
                new ServantDesc(servant.state(), servant.acquired(), true));
    }

    @Override
    public void freeFromTask(String workflowName, String servantId) {
        var servant = storage.get(new ServantKey(workflowName, servantId));
        if (servant == null) {
            return;
        }
        storage.put(new ServantKey(workflowName, servantId),
                new ServantDesc(servant.state, servant.acquired, false));
    }

    @Override
    public synchronized void invalidate(Servant servant, String description) {
        var state = new ServantState.ServantStateBuilder(servant.id(), servant.workflowName(),
                servant.provisioning(), ServantState.Status.DESTROYED)
                .setErrorDescription(description)
                .build();
        storage.put(new ServantKey(servant.workflowName(), servant.id()),
                new ServantDesc(state, false, false));
    }

    @Override
    public List<Servant> get(String workflowName) throws DaoException {
        return storage.values().stream()
            .filter(t -> t.state().workflowName().equals(workflowName))
            .map(s -> (Servant) new ServantImpl(s.state(), queueManager.get(s.state.workflowName(), s.state.id())))
            .toList();
    }

    private record ServantKey(String workflowId, String servantId) {}
    private record ServantDesc(ServantState state, boolean acquired, boolean acquiredForTask) {}

    private boolean contains(Provisioning p1, Provisioning p2) {
        return p1.tags().containsAll(p2.tags());
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
