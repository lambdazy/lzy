package ai.lzy.longrunning;

import ai.lzy.longrunning.task.OperationTask;
import ai.lzy.longrunning.task.dao.OperationTaskDaoImpl;
import ai.lzy.model.db.StorageImpl;
import ai.lzy.model.db.test.DatabaseTestUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationTaskDaoImplTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private StorageImpl storage;
    private OperationTaskDaoImpl taskDao;


    @Before
    public void setup() {
        storage = new StorageImpl(DatabaseTestUtils.preparePostgresConfig(db.getConnectionInfo()),
            "classpath:db/migrations") {};
        taskDao = new OperationTaskDaoImpl(storage, new ObjectMapper());
    }

    @After
    public void teardown() {
        DatabaseTestUtils.cleanup(storage);
        storage.close();
    }

    @Test
    public void create() throws Exception {
        var task = taskDao.insert(OperationTask.createPending("foo", "bar", "MOUNT", Map.of("foo", "bar")), null);
        var fetched = taskDao.get(task.id(), null);
        assertEquals(task, fetched);
    }

    @Test
    public void multiCreate() throws Exception {
        var task1 = taskDao.insert(OperationTask.createPending("foo", "bar", "MOUNT", Map.of("foo", "bar")), null);
        var task2 = taskDao.insert(OperationTask.createPending("foo", "bar", "MOUNT", Map.of("foo", "bar")), null);
        var task3 = taskDao.insert(OperationTask.createPending("foo", "bar", "MOUNT", Map.of("foo", "bar")), null);
        Assert.assertTrue(task1.id() < task2.id());
        Assert.assertTrue(task2.id() < task3.id());
    }

    @Test
    public void update() throws Exception {
        var task = taskDao.insert(OperationTask.createPending("foo", "bar", "MOUNT", Map.of("foo", "bar")), null);
        var updated = taskDao.update(task.id(), OperationTask.Update.builder().status(OperationTask.Status.RUNNING).build(), null);
        assertEquals(OperationTask.Status.RUNNING, updated.status());
        updated = taskDao.update(task.id(), OperationTask.Update.builder().operationId("42").build(), null);
        assertEquals("42", updated.operationId());
        updated = taskDao.update(task.id(), OperationTask.Update.builder().metadata(Map.of("qux", "quux")).build(), null);
        assertEquals(Map.of("qux", "quux"), updated.metadata());
        updated = taskDao.update(task.id(), OperationTask.Update.builder()
            .status(OperationTask.Status.FINISHED)
            .operationId("0")
            .metadata(Map.of())
            .build(), null);
        assertEquals(OperationTask.Status.FINISHED, updated.status());
        assertEquals("0", updated.operationId());
        assertEquals(Map.of(), updated.metadata());
    }

    @Test
    public void delete() throws Exception {
        var task = taskDao.insert(OperationTask.createPending("foo", "bar", "MOUNT", Map.of("foo", "bar")), null);
        taskDao.delete(task.id(), null);
    }

    @Test
    public void updateLease() throws Exception {
        var task = taskDao.insert(OperationTask.createPending("foo", "bar", "MOUNT", Map.of("foo", "bar")), null);
        var updateLease = taskDao.updateLease(task.id(), Duration.ofMinutes(5), null);
        var between = Duration.between(task.createdAt(), updateLease.leaseTill());
        //leaseTill should be around 5 minutes from now
        assertTrue(between.compareTo(Duration.ofMinutes(5)) >= 0);
    }

    @Test
    public void getUnknown() throws Exception {
        var task = taskDao.get(42, null);
        Assert.assertNull(task);
    }

    @Test
    public void updateUnknown() throws Exception {
        var updated = taskDao.update(42, OperationTask.Update.builder().status(OperationTask.Status.RUNNING).build(), null);
        Assert.assertNull(updated);
    }

    @Test
    public void deleteUnknown() throws Exception {
        taskDao.delete(42, null);
    }

    @Test
    public void updateLeaseUnknown() throws Exception {
        var updated = taskDao.updateLease(42, Duration.ofMinutes(5), null);
        Assert.assertNull(updated);
    }

    @Test
    public void lockPendingBatch() throws Exception {
        var task1 = taskDao.insert(OperationTask.createPending("task1", "1", "MOUNT", Map.of()), null);
        var task2 = taskDao.insert(OperationTask.createPending("task2", "2", "MOUNT", Map.of()), null);
        var task3 = taskDao.insert(OperationTask.createPending("task3", "3", "MOUNT", Map.of()), null);
        var task4 = taskDao.insert(OperationTask.createPending("task4", "4", "MOUNT", Map.of()), null);
        var task5 = taskDao.insert(OperationTask.createPending("task5", "1", "MOUNT", Map.of()), null);
        var task6 = taskDao.insert(OperationTask.createPending("task6", "2", "MOUNT", Map.of()), null);
        var task7 = taskDao.insert(OperationTask.createPending("task7", "3", "MOUNT", Map.of()), null);
        var task8 = taskDao.insert(OperationTask.createPending("task8", "4", "MOUNT", Map.of()), null);

        task2 = taskDao.update(task2.id(), statusUpdate(OperationTask.Status.RUNNING), null);
        task3 = taskDao.update(task3.id(), statusUpdate(OperationTask.Status.FINISHED), null);
        task4 = taskDao.update(task4.id(), statusUpdate(OperationTask.Status.FAILED), null);

        var lockedTasks = taskDao.lockPendingBatch("worker", Duration.ofMinutes(5), 10, null);
        var lockedTaskIds = lockedTasks.stream().map(OperationTask::id).collect(Collectors.toSet());
        assertEquals(Set.of(task1.id(), task7.id(), task8.id()), lockedTaskIds);
    }

    @Test
    public void lockPendingBatchWithAllRunning() throws Exception {
        var task1 = taskDao.insert(OperationTask.createPending("task1", "1", "MOUNT", Map.of()), null);
        var task2 = taskDao.insert(OperationTask.createPending("task2", "2", "MOUNT", Map.of()), null);
        var task3 = taskDao.insert(OperationTask.createPending("task3", "3", "MOUNT", Map.of()), null);
        var task4 = taskDao.insert(OperationTask.createPending("task4", "4", "MOUNT", Map.of()), null);
        var task5 = taskDao.insert(OperationTask.createPending("task5", "1", "MOUNT", Map.of()), null);
        var task6 = taskDao.insert(OperationTask.createPending("task6", "2", "MOUNT", Map.of()), null);
        var task7 = taskDao.insert(OperationTask.createPending("task7", "3", "MOUNT", Map.of()), null);
        var task8 = taskDao.insert(OperationTask.createPending("task8", "4", "MOUNT", Map.of()), null);

        task1 = taskDao.update(task1.id(), statusUpdate(OperationTask.Status.RUNNING), null);
        task2 = taskDao.update(task2.id(), statusUpdate(OperationTask.Status.RUNNING), null);
        task3 = taskDao.update(task3.id(), statusUpdate(OperationTask.Status.RUNNING), null);
        task4 = taskDao.update(task4.id(), statusUpdate(OperationTask.Status.RUNNING), null);

        var lockedTasks = taskDao.lockPendingBatch("worker", Duration.ofMinutes(5), 10, null);
        assertTrue(lockedTasks.isEmpty());
    }

    private OperationTask.Update statusUpdate(OperationTask.Status status) {
        return OperationTask.Update.builder().status(status).build();
    }

}
