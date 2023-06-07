package ai.lzy.service.operations;

import ai.lzy.common.IdGenerator;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.service.BeanFactory;
import ai.lzy.service.LzyServiceMetrics;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.ExecutionOperationsDao;
import ai.lzy.service.dao.GraphDao;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import io.grpc.Status;

public abstract class ExecutionOperationRunner extends OperationRunnerBase {
    private final String instanceId;

    private final ExecutionStepContext stepCtx;
    private final LzyServiceMetrics metrics;

    private final BeanFactory.S3SinkClient s3SinkClient;
    private final KafkaAdminClient kafkaClient;
    private final AllocatorBlockingStub allocClient;
    private final SubjectServiceGrpcClient subjClient;

    private final OperationsExecutor executor;
    private final OperationRunnersFactory opRunnersFactory;

    protected ExecutionOperationRunner(ExecutionOperationRunnerBuilder<?> builder) {
        super(builder.id, builder.description, builder.storage, builder.operationsDao, builder.executor);
        this.instanceId = builder.instanceId;
        this.stepCtx = new ExecutionStepContext(builder.id, builder.userId, builder.wfName, builder.execId,
            builder.storage, builder.wfDao, builder.execDao, builder.graphDao, builder.execOpsDao,
            builder.internalUserCredentials, builder.idempotencyKey,
            sre -> fail(sre.getStatus()) ? StepResult.FINISH : StepResult.RESTART, log(), logPrefix(),
            builder.idGenerator);
        this.metrics = builder.metrics;
        this.s3SinkClient = builder.s3SinkClient;
        this.kafkaClient = builder.kafkaClient;
        this.subjClient = builder.subjClient;
        this.allocClient = builder.allocClient;
        this.executor = builder.executor;
        this.opRunnersFactory = builder.opRunnersFactory;
    }

    protected abstract boolean fail(Status status);

    protected ExecutionStepContext stepCtx() {
        return stepCtx;
    }

    protected String instanceId() {
        return instanceId;
    }

    protected String userId() {
        return stepCtx.userId();
    }

    protected String wfName() {
        return stepCtx.wfName();
    }

    protected String execId() {
        return stepCtx.execId();
    }

    protected Storage storage() {
        return stepCtx.storage();
    }

    protected WorkflowDao wfDao() {
        return stepCtx.wfDao();
    }

    protected ExecutionDao execDao() {
        return stepCtx.execDao();
    }

    protected GraphDao graphDao() {
        return stepCtx.graphDao();
    }

    protected ExecutionOperationsDao execOpsDao() {
        return stepCtx.execOpsDao();
    }

    protected LzyServiceMetrics metrics() {
        return metrics;
    }

    protected BeanFactory.S3SinkClient s3SinkClient() {
        return s3SinkClient;
    }

    protected KafkaAdminClient kafkaClient() {
        return kafkaClient;
    }

    protected AllocatorBlockingStub allocClient() {
        return allocClient;
    }

    protected SubjectServiceGrpcClient subjClient() {
        return subjClient;
    }

    protected OperationsExecutor opsExecutor() {
        return executor;
    }

    protected OperationRunnersFactory opRunnersFactory() {
        return opRunnersFactory;
    }

    protected abstract static class ExecutionOperationRunnerBuilder<T extends ExecutionOperationRunnerBuilder<T>> {
        private String instanceId;
        private String id;
        private String description;
        private String idempotencyKey;
        private String userId;
        private String wfName;
        private String execId;
        private Storage storage;
        private WorkflowDao wfDao;
        private GraphDao graphDao;
        private ExecutionDao execDao;
        private OperationDao operationsDao;
        private ExecutionOperationsDao execOpsDao;
        private RenewableJwt internalUserCredentials;
        private IdGenerator idGenerator;
        private OperationsExecutor executor;
        private LzyServiceMetrics metrics;
        private BeanFactory.S3SinkClient s3SinkClient;
        private KafkaAdminClient kafkaClient;
        private AllocatorBlockingStub allocClient;
        private SubjectServiceGrpcClient subjClient;
        private OperationRunnersFactory opRunnersFactory;

        public abstract ExecutionOperationRunner build();

        public T setInstanceId(String id) {
            this.instanceId = id;
            return self();
        }

        public T setId(String id) {
            this.id = id;
            return self();
        }

        public T setDescription(String description) {
            this.description = description;
            return self();
        }

        public T setIdempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return self();
        }

        public T setUserId(String userId) {
            this.userId = userId;
            return self();
        }

        public T setWfName(String wfName) {
            this.wfName = wfName;
            return self();
        }

        public T setExecId(String execId) {
            this.execId = execId;
            return self();
        }

        public T setStorage(Storage storage) {
            this.storage = storage;
            return self();
        }

        public T setWfDao(WorkflowDao wfDao) {
            this.wfDao = wfDao;
            return self();
        }

        public T setGraphDao(GraphDao graphDao) {
            this.graphDao = graphDao;
            return self();
        }

        public T setExecDao(ExecutionDao execDao) {
            this.execDao = execDao;
            return self();
        }

        public T setOperationsDao(OperationDao operationsDao) {
            this.operationsDao = operationsDao;
            return self();
        }

        public T setExecOpsDao(ExecutionOperationsDao execOpsDao) {
            this.execOpsDao = execOpsDao;
            return self();
        }

        public T setIdGenerator(IdGenerator idGenerator) {
            this.idGenerator = idGenerator;
            return self();
        }

        public T setExecutor(OperationsExecutor executor) {
            this.executor = executor;
            return self();
        }

        public T setInternalUserCredentials(RenewableJwt internalUserCredentials) {
            this.internalUserCredentials = internalUserCredentials;
            return self();
        }

        public T setMetrics(LzyServiceMetrics metrics) {
            this.metrics = metrics;
            return self();
        }

        public T setS3SinkClient(BeanFactory.S3SinkClient s3SinkClient) {
            this.s3SinkClient = s3SinkClient;
            return self();
        }

        public T setKafkaClient(KafkaAdminClient kafkaClient) {
            this.kafkaClient = kafkaClient;
            return self();
        }

        public T setSubjClient(SubjectServiceGrpcClient subjClient) {
            this.subjClient = subjClient;
            return self();
        }

        public T setAllocClient(AllocatorBlockingStub allocClient) {
            this.allocClient = allocClient;
            return self();
        }

        public T setOpRunnersFactory(OperationRunnersFactory opRunnersFactory) {
            this.opRunnersFactory = opRunnersFactory;
            return self();
        }

        protected T self() {
            //noinspection unchecked
            return (T) this;
        }
    }
}
