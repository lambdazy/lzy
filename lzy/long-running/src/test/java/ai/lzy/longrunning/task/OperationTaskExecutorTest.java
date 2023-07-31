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
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class OperationTaskExecutorTest {

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
    private OperationTaskExecutor taskExecutor;
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
        taskExecutor = new OperationTaskExecutor(taskDao, operationsExecutor, taskResolver, Duration.ZERO,
            SCHEDULER_DELAY, storage, new StubMetricsProvider(), WORKER_ID , LEASE_DURATION,
            BATCH_SIZE, MAX_RUNNING_TASKS);

    }

    @After
    public void teardown() {
        DatabaseTestUtils.cleanup(storage);
        storage.close();
        taskExecutor.shutdown();
    }

    @Test
    public void executorWorkflow() {
        taskExecutor.start();
    }

    @Test
    public void executorCannotBeStartedTwice() {
        taskExecutor.start();
        assertThrows(IllegalStateException.class, () -> taskExecutor.start());
    }

    @Test
    public void executorShouldWork() throws SQLException {
        taskResolver.addResolver(resolver(MOUNT_TASK_TYPE, () -> OperationRunnerBase.StepResult.FINISH, true));
        var op = createOperation();
        var task = taskExecutor.saveTask(OperationTask.createPending("Test", "foo",
            MOUNT_TASK_TYPE, Map.of(), op.id()), null);
        taskExecutor.start();
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
    public void executorCanFail() throws SQLException {
        taskResolver.addResolver(resolver(MOUNT_TASK_TYPE, () -> OperationRunnerBase.StepResult.FINISH, false));
        var op = createOperation();
        var task = taskExecutor.saveTask(OperationTask.createPending("Test", "foo",
            MOUNT_TASK_TYPE, Map.of(), op.id()), null);
        taskExecutor.start();
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
                    operationsExecutor, taskExecutor));
            }
        };
    }
}
