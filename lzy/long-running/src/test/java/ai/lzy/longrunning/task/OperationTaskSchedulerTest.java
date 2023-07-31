package ai.lzy.longrunning.task;

import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.longrunning.task.dao.OperationTaskDaoImpl;
import ai.lzy.model.db.StorageImpl;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.test.DatabaseTestUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import jakarta.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class OperationTaskSchedulerTest {

    public static final String MOUNT_TASK_TYPE = "MOUNT";
    public static final Duration SCHEDULER_DELAY = Duration.ofMinutes(5);
    public static final Duration LEASE_DURATION = Duration.ofMinutes(5);
    public static final int BATCH_SIZE = 7;
    public static final int MAX_RUNNING_TASKS = 10;
    public static final String WORKER_ID = "worker-42";

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    @Rule
    public Timeout timeout = Timeout.seconds(30);

    private StorageImpl storage;
    private OperationTaskDaoImpl taskDao;
    private OperationTaskScheduler taskScheduler;
    private OperationDaoImpl operationDao;
    private OperationsExecutor operationsExecutor;
    private DispatchingOperationTaskResolver taskResolver;

    @Before
    public void setup() {
        storage = new StorageImpl(DatabaseTestUtils.preparePostgresConfig(db.getConnectionInfo()),
            "classpath:db/migrations") {};
        taskDao = new OperationTaskDaoImpl(storage, new ObjectMapper());
        operationDao = new OperationDaoImpl(storage);
        operationsExecutor = new OperationsExecutor(5, 10, () -> {}, e -> false);
        taskResolver = new DispatchingOperationTaskResolver(List.of());
        taskScheduler = new OperationTaskScheduler(taskDao, operationsExecutor, taskResolver, Duration.ZERO,
            SCHEDULER_DELAY, storage, new StubMetricsProvider(), WORKER_ID , LEASE_DURATION,
            BATCH_SIZE, MAX_RUNNING_TASKS);

    }

    @After
    public void teardown() {
        DatabaseTestUtils.cleanup(storage);
        storage.close();
        taskScheduler.shutdown();
    }

    @Test
    public void schedulerWorkflow() {
        taskScheduler.start();
    }

    @Test
    public void schedulerCannotBeStartedTwice() {
        taskScheduler.start();
        assertThrows(IllegalStateException.class, () -> taskScheduler.start());
    }

    @Test
    public void schedulerShouldWork() throws SQLException {
        taskResolver.addResolver(resolver(MOUNT_TASK_TYPE, () -> OperationRunnerBase.StepResult.FINISH, true));
        var op = createOperation();
        var task = taskScheduler.saveTask(OperationTask.createPending("Test", "foo",
            MOUNT_TASK_TYPE, Map.of(), op.id()), null);
        taskScheduler.start();
        op = waitForOperation(op.id());
        assertNull(op.error());
        task = taskDao.get(task.id(), null);
        assertNotNull(task);
        assertEquals(OperationTask.Status.FINISHED, task.status());
        assertTrue(task.createdAt().isBefore(task.updatedAt()));
        assertNotNull(task.leaseTill());
        assertTrue(task.createdAt().isBefore(task.leaseTill()));
        assertEquals(WORKER_ID, task.workerId());
    }

    @Test
    public void schedulerCanFail() throws SQLException {
        taskResolver.addResolver(resolver(MOUNT_TASK_TYPE, () -> OperationRunnerBase.StepResult.FINISH, false));
        var op = createOperation();
        var task = taskScheduler.saveTask(OperationTask.createPending("Test", "foo",
            MOUNT_TASK_TYPE, Map.of(), op.id()), null);
        taskScheduler.start();
        op = waitForOperation(op.id());
        assertNotNull(op.error());
        task = taskDao.get(task.id(), null);
        assertNotNull(task);
        assertEquals(OperationTask.Status.FAILED, task.status());
        assertTrue(task.createdAt().isBefore(task.updatedAt()));
        assertNotNull(task.leaseTill());
        assertTrue(task.createdAt().isBefore(task.leaseTill()));
        assertEquals(WORKER_ID, task.workerId());
    }

    @Test
    public void schedulerWillLoadOnlyOneBatch() throws SQLException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        taskResolver.addResolver(resolver(MOUNT_TASK_TYPE, () -> {
            latch.countDown();
            return OperationRunnerBase.StepResult.FINISH;
        }, true));
        for (int i = 0; i < MAX_RUNNING_TASKS; i++) {
            var op = createOperation();
            var task = taskScheduler.saveTask(OperationTask.createPending("Test", "foo" + i,
                MOUNT_TASK_TYPE, Map.of(), op.id()), null);
        }
        taskScheduler.start();
        latch.await();
        var tasks = taskDao.getAll(null);
        var tasksByPendingStatus = tasks.stream()
            .collect(Collectors.partitioningBy(x -> x.status() == OperationTask.Status.PENDING));
        var pendingTasks = tasksByPendingStatus.get(true);
        var runningTasks = tasksByPendingStatus.get(false);
        assertEquals(MAX_RUNNING_TASKS - BATCH_SIZE, pendingTasks.size());
        assertEquals(BATCH_SIZE, runningTasks.size());

    }

    private Operation waitForOperation(String opId) throws SQLException {
        while (true) {
            var op = operationDao.get(opId, null);
            if (op == null) {
                fail("Operation " + opId + " cannot be null");
            }
            if (op.done()) {
                return op;
            }
            LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
        }
    }

    private Operation createOperation() throws SQLException {
        var operation = Operation.create("foo", "op", Duration.ofDays(1), null, null);
        operationDao.create(operation, null);
        return operation;
    }

    private TypedOperationTaskResolver resolver(String type,
                                                Supplier<OperationRunnerBase.StepResult> action,
                                                boolean completeOperation) {
        return new TypedOperationTaskResolver() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public Result resolve(OperationTask task, @Nullable TransactionHandle tx) {
                return Result.success(new TestAction(action, completeOperation, task, taskDao,
                    SCHEDULER_DELAY, task.operationId(), "Test action", storage, operationDao,
                    operationsExecutor, taskScheduler));
            }
        };
    }
}
