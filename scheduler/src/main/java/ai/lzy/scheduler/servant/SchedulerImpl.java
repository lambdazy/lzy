package ai.lzy.scheduler.servant;

import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.task.Task;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class SchedulerImpl implements Scheduler {
    private final ServantDao dao;
    private final TaskDao taskDao;
    private final ServantEventDao eventDao;
    private final ServantEventProcessor processor;
    private static final Logger LOG = LogManager.getLogger(SchedulerImpl.class);

    @Inject
    public SchedulerImpl(ServantDao dao, TaskDao taskDao, ServantEventDao eventDao,
                         ServantEventProcessor processor, ServiceConfig config) {
        this.dao = dao;
        this.taskDao = taskDao;
        this.processor = processor;
        this.eventDao = eventDao;
    }

    @Override
    public Task execute(String workflowId, TaskDesc taskDesc) throws StatusException {
        try {
            Task task = taskDao.create(workflowId, taskDesc);
            Servant servant;
            try {
                servant = dao.acquireForTask(workflowId, task.taskId(),
                    taskDesc.zygote().provisioning(), Status.RUNNING, Status.IDLE);
            } catch (DaoException e) {
                servant = null;
            }
            if (servant == null) {
                try {
                    servant = dao.create(workflowId, taskDesc.zygote().provisioning(), taskDesc.zygote().env());
                } catch (DaoException e) {
                    throw io.grpc.Status.INTERNAL.withDescription("Cannot create servant").asException();
                }
            }
            servant.allocate();
            dao.acquireForTask(servant, task.taskId());
            return task;
        } catch (DaoException e) {
            throw io.grpc.Status.INTERNAL.withDescription("Something went wrong").asException();
        }
    }

    @Override
    public void signal(String workflowId, String taskId, int signalNumber, String issue) throws StatusException {
        final Task task = taskDao.get(workflowId, taskId);
        if (task == null) {
            throw io.grpc.Status.NOT_FOUND.withDescription("Task not found").asException();
        }

        if (task.servantId() == null) {
            throw io.grpc.Status.FAILED_PRECONDITION.withDescription("Task is not assigned to servant")
                .asException();
        }

        final Servant servant = dao.get(workflowId, task.servantId());
        if (servant == null) {
            throw io.grpc.Status.INTERNAL.withDescription("Task is assigned to servant, but servant is not exists")
                .asException();
        }

        servant.signal(signalNumber);
    }

    @Override
    public void gracefulStop() {
    }
}
