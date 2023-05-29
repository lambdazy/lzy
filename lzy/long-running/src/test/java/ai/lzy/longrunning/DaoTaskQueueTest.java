package ai.lzy.longrunning;

import ai.lzy.longrunning.task.DaoTaskQueue;
import ai.lzy.longrunning.task.Task;
import ai.lzy.longrunning.task.dao.TaskDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class DaoTaskQueueTest {

    private static final int MAX_QUEUE_SIZE = 10;
    private static final Duration INITIAL_LEASE_TIME = Duration.of(5, ChronoUnit.MINUTES);
    private static final String INSTANCE_ID = "worker";

    private DaoTaskQueue taskQueue;
    private TaskDao taskDaoMock;

    @Before
    public void setup() {
        taskDaoMock = Mockito.mock(TaskDao.class);
        taskQueue = new DaoTaskQueue(taskDaoMock, MAX_QUEUE_SIZE, INITIAL_LEASE_TIME,
            INSTANCE_ID);
    }

    @Test
    public void add() throws Exception {
        Mockito.when(taskDaoMock.insert(Mockito.any(), Mockito.any())).thenReturn(null);
        var task = Task.createPending("task", "1", "MOUNT", Map.of());
        taskQueue.add(task);
        Mockito.verify(taskDaoMock, Mockito.only()).insert(task, null);
    }

    @Test
    public void update() throws Exception {
        Mockito.when(taskDaoMock.update(Mockito.anyLong(), Mockito.any(), Mockito.any())).thenReturn(null);
        var task = Task.createPending("task", "1", "MOUNT", Map.of());
        var update = Task.Update.builder().status(Task.Status.RUNNING).build();
        taskQueue.update(task.id(), update);
        Mockito.verify(taskDaoMock, Mockito.only()).update(task.id(), update, null);
    }

    @Test
    public void updateLease() throws Exception {
        Mockito.when(taskDaoMock.update(Mockito.anyLong(), Mockito.any(), Mockito.any())).thenReturn(null);
        var task = Task.createPending("task", "1", "MOUNT", Map.of());
        var duration = Duration.ofSeconds(42);
        taskQueue.updateLease(task.id(), duration);
        Mockito.verify(taskDaoMock, Mockito.only()).updateLease(task.id(), duration, null);
    }

    @Test
    public void pollNext() throws Exception {
        var tasks = List.of(
            Task.createPending("task1", "1", "MOUNT", Map.of()),
            Task.createPending("task2", "2", "MOUNT", Map.of()),
            Task.createPending("task3", "3", "MOUNT", Map.of())
        );
        Mockito.when(taskDaoMock.lockPendingBatch(Mockito.anyString(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
            .thenReturn(tasks);
        var polledTask1 = taskQueue.pollNext();
        Mockito.verify(taskDaoMock, Mockito.only())
            .lockPendingBatch(INSTANCE_ID, INITIAL_LEASE_TIME, MAX_QUEUE_SIZE, null);
        var polledTask2 = taskQueue.pollNext();
        var polledTask3 = taskQueue.pollNext();
        Mockito.verify(taskDaoMock, Mockito.only())
            .lockPendingBatch(INSTANCE_ID, INITIAL_LEASE_TIME, MAX_QUEUE_SIZE, null);
        Assert.assertEquals(tasks.get(0), polledTask1);
        Assert.assertEquals(tasks.get(1), polledTask2);
        Assert.assertEquals(tasks.get(2), polledTask3);

        var anotherBatch = List.of(Task.createPending("task4", "4", "MOUNT", Map.of()));
        Mockito.when(taskDaoMock.lockPendingBatch(Mockito.anyString(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
            .thenReturn(anotherBatch);
        var polledTask4 = taskQueue.pollNext();
        Mockito.verify(taskDaoMock, Mockito.times(2))
            .lockPendingBatch(INSTANCE_ID, INITIAL_LEASE_TIME, MAX_QUEUE_SIZE, null);
        Assert.assertEquals(anotherBatch.get(0), polledTask4);

        Mockito.when(taskDaoMock.lockPendingBatch(Mockito.anyString(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
            .thenReturn(List.of());
        var polledTask5 = taskQueue.pollNext();
        Mockito.verify(taskDaoMock, Mockito.times(3))
            .lockPendingBatch(INSTANCE_ID, INITIAL_LEASE_TIME, MAX_QUEUE_SIZE, null);
        Assert.assertNull(polledTask5);
    }

    @Test
    public void delete() throws Exception {
        Mockito.doNothing().when(taskDaoMock).delete(Mockito.anyLong(), Mockito.any());
        var task = Task.createPending("task", "1", "MOUNT", Map.of());
        taskQueue.delete(task.id());
        Mockito.verify(taskDaoMock, Mockito.only()).delete(task.id(), null);
    }

    @Test
    public void pollRemaining() throws Exception {
        var tasks = List.of(
            Task.createPending("task1", "1", "MOUNT", Map.of()),
            Task.createPending("task2", "2", "MOUNT", Map.of()),
            Task.createPending("task3", "3", "MOUNT", Map.of())
        );
        Mockito.when(taskDaoMock.lockPendingBatch(Mockito.anyString(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
            .thenReturn(tasks);
        var task = taskQueue.pollNext();
        Assert.assertEquals(tasks.get(0), task);
        Mockito.verify(taskDaoMock, Mockito.only())
            .lockPendingBatch(INSTANCE_ID, INITIAL_LEASE_TIME, MAX_QUEUE_SIZE, null);

        var remainingTasks = taskQueue.pollRemaining();
        Mockito.verify(taskDaoMock, Mockito.only())
            .lockPendingBatch(INSTANCE_ID, INITIAL_LEASE_TIME, MAX_QUEUE_SIZE, null);
        Assert.assertEquals(tasks.subList(1, 3), remainingTasks);

        Mockito.when(taskDaoMock.lockPendingBatch(Mockito.anyString(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
            .thenReturn(List.of());
        var newTasks = taskQueue.pollRemaining();
        Mockito.verify(taskDaoMock, Mockito.times(2))
            .lockPendingBatch(INSTANCE_ID, INITIAL_LEASE_TIME, MAX_QUEUE_SIZE, null);
        Assert.assertEquals(List.of(), newTasks);
    }

}
