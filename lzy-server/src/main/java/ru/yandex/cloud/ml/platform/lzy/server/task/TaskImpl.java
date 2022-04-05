package ru.yandex.cloud.ml.platform.lzy.server.task;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.*;
import ru.yandex.cloud.ml.platform.lzy.model.channel.ChannelSpec;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocator;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import ru.yandex.cloud.ml.platform.lzy.server.local.ServantEndpoint;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ExecutionConcluded;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TaskImpl implements Task {
    private static final Logger LOG = LogManager.getLogger(TaskImpl.class);

    protected final String owner;
    protected final UUID tid;
    protected final URI serverURI;
    private final Zygote workload;
    private final Map<Slot, String> assignments;
    private final ChannelsManager channels;
    private final List<Consumer<Tasks.TaskProgress>> listeners = new ArrayList<>();
    private final Map<Slot, ChannelSpec> attachedSlots = new HashMap<>();
    // [TODO] in case of disconnected/retry cycle it seems, that this pattern is completely incorrect
    private CompletableFuture<ServantsAllocator.ServantConnection> servant = new CompletableFuture<>();
    private final String bucket;
    private State state = State.PREPARING;

    public TaskImpl(String owner, UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        ChannelsManager channels,
        URI serverURI,
        String bucket
    ) {
        this.owner = owner;
        this.tid = tid;
        this.workload = workload;
        this.assignments = assignments;
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
    public void state(State newState, String... description) {
        if (newState != state) {
            state = newState;
            progress(Tasks.TaskProgress.newBuilder()
                .setStatus(Tasks.TaskProgress.Status.valueOf(newState.name()))
                .setDescription(String.join("\n", description))
                .build());
        }
    }

    @Override
    public void onProgress(Consumer<Tasks.TaskProgress> listener) {
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
    public void attachServant(ServantsAllocator.ServantConnection connection) {
        this.servant.complete(connection);
        LOG.info("Server is attached to servant {}", connection.uri());
        final Tasks.TaskSpec.Builder taskSpecBuilder = Tasks.TaskSpec.newBuilder();
        taskSpecBuilder.setTid(tid.toString());
        taskSpecBuilder.setZygote(GrpcConverter.to(workload));
        assignments.forEach((slot, binding) -> {
            // need to filter out std* slots because they don't exist on prepare
            if (Stream.of(Slot.STDIN, Slot.STDOUT, Slot.STDERR).map(Slot::name).noneMatch(s -> s.equals(slot.name()))) {
                taskSpecBuilder.addAssignmentsBuilder()
                    .setSlot(GrpcConverter.to(slot))
                    .setBinding(binding)
                    .build();
            }
        });

        connection.onProgress(progress -> {
            switch (progress.getStatusCase()) {
                case ATTACH: {
                    LOG.info("BaseTask::attach " + JsonUtils.printRequest(progress));
                    final Servant.SlotAttach attach = progress.getAttach();
                    final Slot slot = GrpcConverter.from(attach.getSlot());
                    final URI slotUri = URI.create(attach.getUri());
                    final String channelName;
                    if (attach.getChannel().isEmpty()) {
                        final String binding = assignments.getOrDefault(slot, "");
                        channelName = binding.startsWith("channel:") ? binding.substring("channel:".length()) : null;
                    } else {
                        channelName = attach.getChannel();
                    }

                    final ChannelSpec channel = channels.get(channelName);
                    if (channel != null) {
                        attachedSlots.put(slot, channel);
                        try {
                            channels.bind(channel, new ServantEndpoint(slot, slotUri, tid, connection.control()));
                        } catch (ChannelException ce) {
                            LOG.warn("Unable to connect channel " + channelName + " to the slot " + slotUri);
                        }
                    } else {
                        LOG.warn("Unable to attach channel to " + tid + ":" + slot.name()
                            + ". Channel not found.");
                    }
                    return true;
                }
                case DETACH: {
                    LOG.info("BaseTask::detach " + JsonUtils.printRequest(progress));
                    final Servant.SlotDetach detach = progress.getDetach();
                    final Slot slot = GrpcConverter.from(detach.getSlot());
                    final URI slotUri = URI.create(detach.getUri());
                    final Endpoint endpoint = new ServantEndpoint(slot, slotUri, tid, connection.control());
                    final ChannelSpec channel = channels.bound(endpoint);
                    if (channel != null) {
                        attachedSlots.remove(slot);
                        try {
                            channels.unbind(channel, endpoint);
                        } catch (ChannelException ce) {
                            LOG.warn("Unable to unbind slot " + slotUri + " from the channel " + channel.name());
                        }
                    }
                    return true;
                }
                case EXECUTESTART: {
                    LOG.info("Task " + tid + " started");
                    state(State.EXECUTING);
                    return true;
                }
                case EXECUTESTOP: {
                    final ExecutionConcluded executeStop = progress.getExecuteStop();
                    LOG.info("Task " + tid + " exited rc: " + executeStop.getRc());
                    if (executeStop.getRc() != 0) {
                        state(State.ERROR, "Exit code: " + executeStop.getRc(), executeStop.getDescription());
                    } else {
                        state(State.SUCCESS);
                    }
                    break;
                }
                default:
            }
            return false;
        });
        if (!EnumSet.of(State.ERROR, State.SUCCESS).contains(state)) {
            state(State.DISCONNECTED);
        }
        servant = null;
    }


    private void progress(Tasks.TaskProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    @Override
    public URI servantUri() {
        return this.servant.isDone() ? this.servant().uri() : null;
    }

    @Override
    public Stream<Slot> slots() {
        return attachedSlots.keySet().stream();
    }

    @Override
    public SlotStatus slotStatus(Slot slot) throws TaskException {
        final Slot definedSlot = workload.slot(slot.name());
        final ServantsAllocator.ServantConnection servant = servant();
        if (servant == null) {
            if (definedSlot != null) {
                return new PreparingSlotStatus(owner, this, definedSlot, assignments.get(slot));
            }
            throw new TaskException("No such slot: " + tid + ":" + slot);
        }
        final Servant.SlotCommandStatus slotStatus = servant.control().configureSlot(
            Servant.SlotCommand.newBuilder()
                .setTid(tid.toString())
                .setSlot(slot.name())
                .setStatus(Servant.StatusCommand.newBuilder().build())
                .build()
        );
        return GrpcConverter.from(slotStatus.getStatus());
    }

    @Override
    public void signal(TasksManager.Signal signal) throws TaskException {
        final ServantsAllocator.ServantConnection ser = servant();
        LOG.info("Sending signal {} to servant {} for task {}", signal.name(), servant, tid);
        //noinspection ResultOfMethodCallIgnored
        ser.control().signal(Tasks.TaskSignal.newBuilder().setSigValue(signal.sig()).build());
    }

    private ServantsAllocator.ServantConnection servant() {
        try {
            // TODO(d-kruchinin): what if servant won't come with attachServant?
            return servant.get(600, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed to get connection to servant for task {}\nCause: {}", tid, e);
            throw new RuntimeException(e);
        }
    }
}
