package ai.lzy.worker;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.test.GrpcUtils;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.kafka.KafkaAdminClient;
import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.util.kafka.ScramKafkaAdminClient;
import ai.lzy.util.kafka.test.KafkaTestUtils;
import ai.lzy.v1.AllocatorPrivateGrpc;
import ai.lzy.v1.VmAllocatorPrivateApi;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import org.junit.*;
import scala.collection.immutable.Map$;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.*;
import static org.junit.Assert.assertThrows;

public class WorkerTests extends IamOnlyWorkerTests {
    private static String kafkaBootstrapServer;
    private static EmbeddedK kafka;
    private static KafkaAdminClient kafkaAdminClient;

    private static final IdGenerator idGenerator = new RandomIdGenerator();

    private static String allocatorAddress;
    private static Server allocatorServer;

    private static String channelManagerAddress;

    private KafkaTestUtils.ReadKafkaTopicFinisher finishStdlogsReader;
    private LMO.KafkaTopicDescription stdlogsTopic;
    private ArrayBlockingQueue<Object> stdlogs;

    @BeforeClass
    public static void beforeTest() throws Exception {
        var allocatorPort = GrpcUtils.rollPort();
        allocatorAddress = "localhost:" + allocatorPort;
        allocatorServer = newGrpcServer("localhost", allocatorPort, NO_AUTH)
            .addService(new AllocatorPrivateGrpc.AllocatorPrivateImplBase() {
                public void register(VmAllocatorPrivateApi.RegisterRequest request,
                                     StreamObserver<VmAllocatorPrivateApi.RegisterResponse> response)
                {
                    response.onNext(VmAllocatorPrivateApi.RegisterResponse.getDefaultInstance());
                    response.onCompleted();
                }

                public void heartbeat(VmAllocatorPrivateApi.HeartbeatRequest request,
                                      StreamObserver<VmAllocatorPrivateApi.HeartbeatResponse> response)
                {
                    response.onNext(VmAllocatorPrivateApi.HeartbeatResponse.getDefaultInstance());
                    response.onCompleted();
                }
            })
            .build();
        allocatorServer.start();

        channelManagerAddress = "localhost:" + GrpcUtils.rollPort();

        var kafkaPort = GrpcUtils.rollPort();
        var zkPort = GrpcUtils.rollPort();
        kafkaBootstrapServer = "localhost:" + kafkaPort;
        @SuppressWarnings("unchecked")
        var brokerConf = (scala.collection.immutable.Map<String, String>) Map$.MODULE$.<String, String>empty()
            .updated("zookeeper.session.timeout.ms", "60000")
            .updated("zookeeper.connection.timeout.ms", "60000");
        var conf = Map$.MODULE$.<String, String>empty();
        var config = EmbeddedKafkaConfig$.MODULE$.apply(kafkaPort, zkPort, brokerConf, conf, conf);
        kafka = EmbeddedKafka.start(config);
        KafkaHelper.USE_AUTH.set(false);
        kafkaAdminClient = new ScramKafkaAdminClient(KafkaConfig.of(kafkaBootstrapServer));
    }

    @AfterClass
    public static void afterTest() throws InterruptedException {
        allocatorServer.shutdown();
        allocatorServer.awaitTermination();

        kafkaAdminClient.shutdown();
        kafka.stop(true);
        KafkaHelper.USE_AUTH.set(true);
    }

    @Before
    public void before() {
        finishStdlogsReader = new KafkaTestUtils.ReadKafkaTopicFinisher();
        stdlogsTopic = prepareKafkaTopic("kafkauser", "password", idGenerator.generate("stdlogs-", 5));
        stdlogs = KafkaTestUtils.readKafkaTopic(kafkaBootstrapServer, stdlogsTopic.getTopic(), finishStdlogsReader);
    }

    @After
    public void after() {
        finishStdlogsReader.finish();
        dropKafkaTopicSafe(stdlogsTopic.getTopic());
    }

    private static LMO.KafkaTopicDescription prepareKafkaTopic(String username, String password, String topicName) {
        kafkaAdminClient.createTopic(topicName);
        try {
            kafkaAdminClient.createUser(username, password);
        } catch (StatusRuntimeException e) {
            if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                throw e;
            }
        }
        //kafkaAdminClient.grantPermission(username, topicName);

        return LMO.KafkaTopicDescription.newBuilder()
            .addBootstrapServers(kafkaBootstrapServer)
            .setTopic(topicName)
            .setUsername(username)
            .setPassword(password)
            .build();
    }

    private static void dropKafkaTopicSafe(String topicName) {
        try {
            kafkaAdminClient.dropTopic(topicName);
        } catch (StatusRuntimeException ignored) {
            // ignore
        }
    }

    private record WorkerDesc(
        String workerId,
        Worker worker,
        RsaUtils.RsaKeys keys,
        RenewableJwt jwt,
        ManagedChannel workerApiChannel,
        WorkerApiGrpc.WorkerApiBlockingStub workerApiStub,
        LongRunningServiceGrpc.LongRunningServiceBlockingStub workerApiOpStub,
        ManagedChannel slotsApiChannel,
        LzySlotsApiGrpc.LzySlotsApiBlockingStub slotsApiStub,
        LongRunningServiceGrpc.LongRunningServiceBlockingStub slotsApiOpStub
    ) implements AutoCloseable
    {
        WorkerApiGrpc.WorkerApiBlockingStub workerApiStub(@Nullable String token) {
            workerApiChannel.resetConnectBackoff();
            var creds = token != null ? token : jwt.get().token();
            return newBlockingClient(workerApiStub, "x", () -> creds);
        }

        LongRunningServiceGrpc.LongRunningServiceBlockingStub workerApiOpStub(@Nullable String token) {
            workerApiChannel.resetConnectBackoff();
            var creds = token != null ? token : jwt.get().token();
            return newBlockingClient(workerApiOpStub, "x", () -> creds);
        }

        LzySlotsApiGrpc.LzySlotsApiBlockingStub slotsApiStub(@Nullable String token) {
            slotsApiChannel.resetConnectBackoff();
            var creds = token != null ? token : jwt.get().token();
            return newBlockingClient(slotsApiStub, "x", () -> creds);
        }

        @Override
        public void close() {
            worker.stop();
            workerApiChannel.shutdownNow();
            slotsApiChannel.shutdownNow();
            try {
                workerApiChannel.awaitTermination(10, TimeUnit.SECONDS);
                slotsApiChannel.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static JwtCredentials createWorkerCreds(String workerName, String uid, String workflowName)
        throws Exception
    {
        var workerId = idGenerator.generate(workerName + "-");

        var workerKeys = RsaUtils.generateRsaKeys();
        var workerSubject = subjectIamClient.createSubject(AuthProvider.INTERNAL, workerId, SubjectType.WORKER,
            new SubjectCredentials("main", workerKeys.publicKey(), CredentialsType.PUBLIC_KEY));
        abIamClient.setAccessBindings(new Workflow(uid + '/' + workflowName),
            List.of(new AccessBinding(Role.LZY_WORKER, workerSubject)));

        var workerJwt = new RenewableJwt(workerId, "INTERNAL", Duration.ofDays(1),
            CredentialsUtils.readPrivateKey(workerKeys.privateKey()));
        return (JwtCredentials) workerJwt.get();
    }

    private static WorkerTests.WorkerDesc startWorker(String uid, String workflowName) throws Exception {
        var workerId = idGenerator.generate("worker-");
        Worker.selectRandomValues(true);

        var ctx = Worker.startApplication(workerId,
            allocatorAddress, "localhost:" + iamPort, Duration.ofHours(1), channelManagerAddress, "localhost",
            "token-" + workerId, 0, KafkaConfig.of(kafkaBootstrapServer), null, null, null);

        var worker = ctx.getBean(Worker.class);
        var config = ctx.getBean(ServiceConfig.class);

        var workerKeys = RsaUtils.generateRsaKeys();
        var workerSubject = subjectIamClient.createSubject(AuthProvider.INTERNAL, workerId, SubjectType.WORKER,
            new SubjectCredentials("main", workerKeys.publicKey(), CredentialsType.PUBLIC_KEY));
        abIamClient.setAccessBindings(new Workflow(uid + '/' + workflowName),
            List.of(new AccessBinding(Role.LZY_WORKER, workerSubject)));

        var workerJwt = new RenewableJwt(workerId, "INTERNAL", Duration.ofDays(1),
            CredentialsUtils.readPrivateKey(workerKeys.privateKey()));

        var workerApiChannel = newGrpcChannel("localhost:" + config.getApiPort(),
            WorkerApiGrpc.SERVICE_NAME, LongRunningServiceGrpc.SERVICE_NAME);
        var slotsApiChannel = newGrpcChannel("localhost:" + config.getFsPort(),
            LzySlotsApiGrpc.SERVICE_NAME, LongRunningServiceGrpc.SERVICE_NAME);

        var workerApiStub = WorkerApiGrpc.newBlockingStub(workerApiChannel);
        var workerApiOpStub = LongRunningServiceGrpc.newBlockingStub(workerApiChannel);

        var slotsApiStub = LzySlotsApiGrpc.newBlockingStub(slotsApiChannel);
        var slotsApiOpStub = LongRunningServiceGrpc.newBlockingStub(slotsApiChannel);

        return new WorkerTests.WorkerDesc(workerId, worker, workerKeys, workerJwt,
            workerApiChannel, workerApiStub, workerApiOpStub,
            slotsApiChannel, slotsApiStub, slotsApiOpStub);
    }

    @Test
    public void apiAvailability() throws Exception {
        var internalUser = internalUserCredentials.get().token();

        var hackerCreds = createWorkerCreds("hacker-worker", "hacker-user", "wf");

        try (var worker = startWorker("uid", "wf")) {
            // SlotsApi unavailable for any worker
            var e = assertThrows(StatusRuntimeException.class, () ->
                worker.slotsApiStub(null).startTransfer(LSA.StartTransferRequest.getDefaultInstance()));
            Assert.assertEquals(e.toString(), Status.Code.UNAVAILABLE, e.getStatus().getCode());

            e = assertThrows(StatusRuntimeException.class, () ->
                worker.slotsApiStub(null).read(LSA.ReadDataRequest.getDefaultInstance()).next());
            Assert.assertEquals(e.toString(), Status.Code.UNAVAILABLE, e.getStatus().getCode());

            // SlotsApi unavailable for internal user
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.slotsApiStub(internalUser).startTransfer(LSA.StartTransferRequest.getDefaultInstance()));
            Assert.assertEquals(e.toString(), Status.Code.UNAVAILABLE, e.getStatus().getCode());

            // WorkerApi Ops unavailable for any worker
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.workerApiOpStub(null)
                    .get(LongRunning.GetOperationRequest.newBuilder().setOperationId("1").build()));
            Assert.assertEquals(e.toString(), Status.Code.PERMISSION_DENIED, e.getStatus().getCode());

            // WorkerApi Ops unavailable for internal user
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.workerApiOpStub(internalUser).get(
                    LongRunning.GetOperationRequest.newBuilder().setOperationId("1").build()));
            Assert.assertEquals(e.toString(), Status.Code.PERMISSION_DENIED, e.getStatus().getCode());

            // WorkerApi unavailable for any worker
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.workerApiStub(null).execute(LWS.ExecuteRequest.getDefaultInstance()));
            Assert.assertEquals(e.toString(), Status.Code.PERMISSION_DENIED, e.getStatus().getCode());

            // init worker
            {
                var resp = worker.workerApiStub(internalUser)
                    .init(
                        LWS.InitRequest.newBuilder()
                            .setUserId("uid")
                            .setWorkflowName("wf")
                            .setWorkerSubjectName(worker.workerId())
                            .setWorkerPrivateKey(worker.keys().privateKey())
                            .build());
                resp.writeTo(System.err);
            }

            // Worker Ops available for internal user
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.workerApiOpStub(internalUser).get(
                    LongRunning.GetOperationRequest.newBuilder().setOperationId("1").build()));
            Assert.assertEquals(e.toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());

            // SlotsApi control plane available for internal user
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.slotsApiStub(internalUser).startTransfer(LSA.StartTransferRequest.getDefaultInstance()));
            Assert.assertEquals(e.toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());

            // SlotsApi control plane unavailable for worker
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.slotsApiStub(null).startTransfer(LSA.StartTransferRequest.getDefaultInstance()));
            Assert.assertEquals(e.toString(), Status.Code.PERMISSION_DENIED, e.getStatus().getCode());

            // SlotsApi data plane available for internal user (is it a bug?)
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.slotsApiStub(internalUser).read(LSA.ReadDataRequest.getDefaultInstance()).next());
            Assert.assertEquals(e.toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());

            // SlotsApi data plane available for worker
            e = assertThrows(StatusRuntimeException.class, () ->
                worker.slotsApiStub(null).read(LSA.ReadDataRequest.getDefaultInstance()).next());
            Assert.assertEquals(e.toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());

            // hacker attempts
            {
                var hackerToken = hackerCreds.token();

                // no SlotsApi control plane
                e = assertThrows(StatusRuntimeException.class, () ->
                    worker.slotsApiStub(hackerToken).startTransfer(LSA.StartTransferRequest.getDefaultInstance()));
                Assert.assertEquals(e.toString(), Status.Code.PERMISSION_DENIED, e.getStatus().getCode());

                // no SlotsApi data plane
                e = assertThrows(StatusRuntimeException.class, () ->
                    worker.slotsApiStub(hackerToken).read(LSA.ReadDataRequest.getDefaultInstance()).next());
                Assert.assertEquals(e.toString(), Status.Code.PERMISSION_DENIED, e.getStatus().getCode());
            }

            finishStdlogsReader.finish();
        }
    }
}
