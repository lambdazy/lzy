package ai.lzy.longrunning.task;

import ai.lzy.longrunning.task.dao.TaskDao;
import com.google.common.collect.Queues;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Queue;

import static ai.lzy.model.db.DbHelper.withRetries;

public class DaoTaskQueue implements TaskQueue {

    private static final Logger LOG = LogManager.getLogger(TaskExecutor.class);

    private final TaskDao taskDao;
    private final int maxQueueSize;
    private final Duration leaseTime;
    private final String instanceId;
    private final Queue<Task> queue;

    public DaoTaskQueue(TaskDao taskDao, int maxQueueSize, Duration leaseTime, String instanceId) {
        this.taskDao = taskDao;
        this.maxQueueSize = maxQueueSize;
        this.leaseTime = leaseTime;
        this.instanceId = instanceId;
        this.queue = Queues.newConcurrentLinkedQueue();
    }

    private void loadNextBatch() {
        var toLoad = capacity();
        if (toLoad > 0) {
            var tasks = loadPendingTasks(toLoad);
            queue.addAll(tasks);
        }
    }

    private int capacity() {
        return maxQueueSize - queue.size();
    }

    private List<Task> loadPendingTasks(int toLoad) {
        try {
            return withRetries(LOG, () -> taskDao.lockPendingBatch(instanceId, leaseTime, toLoad, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void add(Task task) {
        try {
            withRetries(LOG, () -> taskDao.insert(task, null));
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Nullable
    @Override
    public Task pollNext() {
        var next = queue.poll();
        if (next == null) {
            loadNextBatch();
            next = queue.poll();
        }
        return next;
    }

    public List<Task> pollRemaining() {
        if (!queue.isEmpty()) {
            var result = queue.stream().toList();
            queue.clear();
            return result;
        }
        //don't load to queue, just return loaded tasks
        return loadPendingTasks(capacity());
    }

    @Override
    public void update(long id, Task.Update update) {
        try {
            withRetries(LOG, () -> taskDao.update(id, update, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateLease(Task task, Duration duration) {
        try {
            withRetries(LOG, () -> taskDao.updateLease(task.id(), duration, null));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
