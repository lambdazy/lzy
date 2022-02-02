package ru.yandex.cloud.ml.platform.lzy.server.task;


import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import ru.yandex.cloud.ml.platform.lzy.server.local.ServantEndpoint;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ExecutionConcluded;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ExecutionProgress;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.ContextSpec;

public abstract class BaseTask implements Task {

    private static final Logger LOG = LogManager.getLogger(BaseTask.class);

    protected final String owner;
    protected final UUID tid;
    protected final URI serverURI;
    @Nonnull
    protected final SnapshotMeta snapshotMeta;
    private final Zygote workload;
    private final Map<Slot, String> assignments;
    private final ChannelsManager channels;
    private final List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();
    private final Map<Slot, Channel> attachedSlots = new HashMap<>();
    private final CompletableFuture<LzyServantBlockingStub> servant = new CompletableFuture<>();
    private final String bucket;
    private final AtomicBoolean alreadyStopped = new AtomicBoolean(false);
    private State state = State.PREPARING;
    private URI servantUri;

    public BaseTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        @Nonnull SnapshotMeta snapshotMeta,
        ChannelsManager channels,
        URI serverURI,
        String bucket
    ) {
        this.owner = owner;
        this.tid = tid;
        this.workload = workload;
        this.assignments = assignments;
        this.snapshotMeta = snapshotMeta;
        this.channels = channels;
        this.serverURI = serverURI;
        this.bucket = bucket;
    }

    @Override
    public String bucket() {
        return bucket;
    }

    @Override
    public UUID tid() {
        return tid;
    }

    @Override
    public Zygote workload() {
        return workload;
    }

    @Override
    public State state() {
        return state;
    }

    @SuppressWarnings("WeakerAccess")
    protected void state(State newState) {
        if (newState != state) {
            state = newState;
            progress(Servant.ExecutionProgress.newBuilder()
                .setChanged(Servant.StateChanged.newBuilder()
                    .setNewState(Servant.StateChanged.State.valueOf(newState.name())).build())
                .build());
        }
    }

    @Override
    public SnapshotMeta wbMeta() {
        return snapshotMeta;
    }

    @Override
    public void onProgress(Consumer<Servant.ExecutionProgress> listener) {
        listeners.add(listener);
    }

    @Override
    public Slot slot(String slotName) {
        return attachedSlots.keySet().stream()
            .filter(s -> s.name().equals(slotName))
            .findFirst()
            .orElse(workload.slot(slotName));
    }

    @Override
    public void attachServant(URI uri, LzyServantBlockingStub servant) {
        servantUri = uri;
        this.servant.complete(servant);
        LOG.info("Server is attached to servant {}", servantUri);
        // will be removed while server refactoring
        final CountDownLatch contextStarted = new CountDownLatch(1);
        final AtomicBoolean contextHasError = new AtomicBoolean(false);
        Operations.Zygote zygote = GrpcConverter.to(workload);
        final Tasks.ContextSpec.Builder contextBuilder = Tasks.ContextSpec.newBuilder()
            .setEnv(zygote.getEnv())
            .setProvisioning(zygote.getProvisioning());
        contextBuilder.setSnapshotMeta(SnapshotMeta.to(snapshotMeta));
        assignments.forEach((slot, binding) ->
            contextBuilder.addAssignmentsBuilder()
                .setSlot(GrpcConverter.to(slot))
                .setBinding(binding)
                .build()
        );

        final ContextSpec spec = contextBuilder.build();

        final Iterator<Servant.ContextProgress> contextProgress = servant.prepare(spec);
        LOG.info("Preparing context " + JsonUtils.printRequest(spec));

        state(State.CONNECTED);

        final ForkJoinTask<?> task =
            ForkJoinPool.commonPool().submit(() -> contextProgress.forEachRemaining(progress -> {
                switch (progress.getStatusCase()) {
                    case ATTACH: {
                        LOG.info("BaseTask::attach " + JsonUtils.printRequest(progress));
                        final Servant.SlotAttach attach = progress.getAttach();
                        final Slot slot = GrpcConverter.from(attach.getSlot());
                        final URI slotUri = URI.create(attach.getUri());
                        final String channelName;
                        if (attach.getChannel().isEmpty()) {
                            final String binding = assignments.getOrDefault(slot, "");
                            channelName = binding.startsWith("channel:")
                                ? binding.substring("channel:".length()) :
                                null;
                        } else {
                            channelName = attach.getChannel();
                        }

                        final Channel channel = channels.get(channelName);
                        if (channel != null) {
                            attachedSlots.put(slot, channel);
                            channels.bind(channel,
                                new ServantEndpoint(slot, slotUri, tid, servant));
                        } else {
                            LOG.warn("Unable to attach channel to " + tid + ":" + slot.name()
                                + ". Channel not found.");
                        }
                        break;
                    }
                    case DETACH: {
                        LOG.info("BaseTask::detach " + JsonUtils.printRequest(progress));
                        final Servant.SlotDetach detach = progress.getDetach();
                        final Slot slot = GrpcConverter.from(detach.getSlot());
                        final URI slotUri = URI.create(detach.getUri());
                        final Endpoint endpoint = new ServantEndpoint(slot, slotUri, tid, servant);
                        final Channel channel = channels.bound(endpoint);
                        if (channel != null) {
                            attachedSlots.remove(slot);
                            channels.unbind(channel, endpoint);
                        }
                        break;
                    }
                    case START: {
                        LOG.info("Context " + uri + " started");
                        contextStarted.countDown();
                        break;
                    }
                    case EXIT: {
                        LOG.info("Context " + uri + " exited");
                        break;
                    }
                    default:
                    case ERROR: {
                        LOG.info("Error in context " + JsonUtils.printRequest(progress));
                        this.progress(
                            ExecutionProgress.newBuilder()
                                .setExit(ExecutionConcluded.newBuilder()
                                    .setDescription(progress.getError().getDescription())
                                    .setRc(progress.getError().getRc())
                                    .build())
                                .build()
                        );
                        contextHasError.set(true);
                        contextStarted.countDown();
                    }
                }
            }));

        try {
            contextStarted.await();
            if (contextHasError.get()) {
                state(State.FINISHED);
                LOG.info("Stopping servant {}", servantUri);
                stopServant();
                return;
            }
        } catch (InterruptedException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }

        final Tasks.TaskSpec.Builder builder = Tasks.TaskSpec.newBuilder()
            .setAuth(IAM.Auth.newBuilder()
                .setTask(IAM.TaskCredentials.newBuilder()
                    .setTaskId(tid.toString())
                    .build())
                .build())
            .setZygote(GrpcConverter.to(workload));

        builder.setSnapshotMeta(SnapshotMeta.to(snapshotMeta));
        assignments.forEach((slot, binding) ->
            builder.addAssignmentsBuilder()
                .setSlot(GrpcConverter.to(slot))
                .setBinding(binding)
                .build()
        );
        final Iterator<Servant.ExecutionProgress> progressIt = servant.execute(builder.build());
        try {
            progressIt.forEachRemaining(progress -> {
                LOG.info("BaseTask::Progress " + JsonUtils.printRequest(progress));
                this.progress(progress);
                switch (progress.getStatusCase()) {
                    case STARTED: {
                        state(State.RUNNING);
                        LOG.info("Execution started");
                        return;
                    }
                    default:
                    case EXIT: {
                        LOG.info("Execution finished");
                        state(State.FINISHED);
                    }
                }
            });
            task.join();
        } catch (Exception e) {
            LOG.error(e);
            throw new RuntimeException(e);
        } finally {
            stopServant();
            state(State.DESTROYED);
        }
    }

    @Override
    public void stopServant() {
        try {
            //noinspection ResultOfMethodCallIgnored
            servant().stop(IAM.Empty.newBuilder().build());
        } catch (Exception e) {
            LOG.warn("Channel for servant uri={} was shutdown; exception={}", servantUri, e);
        }
        LOG.info("Stopped servant {}", servantUri);
        alreadyStopped.set(true);
    }

    @Override
    public boolean servantIsAlive() {
        return servant.isDone() && !alreadyStopped.get();
    }

    private void progress(Servant.ExecutionProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    @Override
    public URI servantUri() {
        return servantUri;
    }

    @Override
    public Stream<Slot> slots() {
        return attachedSlots.keySet().stream();
    }

    @Override
    public SlotStatus slotStatus(Slot slot) throws TaskException {
        final Slot definedSlot = workload.slot(slot.name());
        if (!servantIsAlive()) {
            if (definedSlot != null) {
                return new PreparingSlotStatus(owner, this, definedSlot, assignments.get(slot));
            }
            throw new TaskException("No such slot: " + tid + ":" + slot);
        }
        final Servant.SlotCommandStatus slotStatus = servant().configureSlot(
            Servant.SlotCommand.newBuilder()
                .setSlot(slot.name())
                .setStatus(Servant.StatusCommand.newBuilder().build())
                .build()
        );
        return GrpcConverter.from(slotStatus.getStatus());
    }

    @Override
    public void signal(TasksManager.Signal signal) throws TaskException {
        final LzyServantBlockingStub ser = servant();
        LOG.info("Sending signal {} to servant {} for task {}", signal.name(), servantUri, tid);
        //noinspection ResultOfMethodCallIgnored
        ser.signal(Tasks.TaskSignal.newBuilder().setSigValue(signal.sig()).build());
    }

    private LzyServantBlockingStub servant() {
        try {
            // TODO(d-kruchinin): what if servant won't come with attachServant?
            return servant.get(600, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed to get connection to servant for task {}\nCause: {}", tid, e);
            throw new RuntimeException(e);
        }
    }
}
