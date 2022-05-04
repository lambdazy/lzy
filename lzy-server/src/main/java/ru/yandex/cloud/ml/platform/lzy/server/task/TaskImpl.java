package ru.yandex.cloud.ml.platform.lzy.server.task;


import io.grpc.Context;
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
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ExecutionConcluded;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static ru.yandex.cloud.ml.platform.lzy.server.task.Task.State.*;

public class TaskImpl implements Task {
    private static final Logger LOG = LogManager.getLogger(TaskImpl.class);

    protected final String owner;
    protected final UUID tid;
    protected final URI serverURI;
    private final Zygote workload;
    private final Map<Slot, String> assignments;
    private final ChannelsManager channels;
    private final List<Consumer<Tasks.TaskProgress>> listeners = Collections.synchronizedList(new ArrayList<>());
    private final Map<Slot, ChannelSpec> attachedSlots = new HashMap<>();
    private ServantsAllocator.ServantConnection servant;
    private State state = State.PREPARING;
    private final List<TasksManager.Signal> signalsQueue = new ArrayList<>();

    public TaskImpl(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments,
                    ChannelsManager channels, URI serverURI
    ) {
        this.owner = owner;
        this.tid = tid;
        this.workload = workload;
        this.assignments = assignments;
        this.channels = channels;
        this.serverURI = serverURI;
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

    @Override
    @SuppressWarnings("WeakerAccess")
    public synchronized void state(State newState, int rc, String... description) {
        if (newState != state) {
            state = newState;
            progress(Tasks.TaskProgress.newBuilder()
                .setStatus(Tasks.TaskProgress.Status.valueOf(newState.name()))
                .setDescription(String.join("\n", description))
                .setRc(rc)
                .build());
        }
    }

    @Override
    public synchronized void state(State newState, String... description) {
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
    public synchronized Slot slot(String slotName) {
        return attachedSlots.keySet().stream()
            .filter(s -> s.name().equals(slotName))
            .findFirst()
            .orElse(workload.slot(slotName));
    }

    @Override
    public synchronized void attachServant(ServantsAllocator.ServantConnection connection) {
        LOG.info("Server is attached to servant {}", connection.uri());

        connection.onProgress(progress -> {
            synchronized (TaskImpl.this) {
                switch (progress.getStatusCase()) {
                    case ATTACH: {
                        LOG.info("Attach " + JsonUtils.printRequest(progress));
                        final Servant.SlotAttach attach = progress.getAttach();
                        final Slot slot = GrpcConverter.from(attach.getSlot());
                        final URI slotUri = URI.create(attach.getUri());
                        final String channelName;
                        if (attach.getChannel().isEmpty()) {
                            final String binding = assignments.getOrDefault(slot, "");
                            channelName = binding.startsWith("channel:")
                                ? binding.substring("channel:".length())
                                : null;
                        } else {
                            channelName = attach.getChannel();
                        }

                        final ChannelSpec channel = channels.get(channelName);
                        if (channel != null) {
                            attachedSlots.put(slot, channel);
                            try {
                                channels.bind(channel, new ServantEndpoint(slot, slotUri, tid, connection.control()));
                            } catch (ChannelException ce) {
                                LOG.error(
                                    "Unable to connect channel " + channelName + " to the slot " + slotUri,
                                    ce
                                );
                            }
                        } else {
                            LOG.error("Unable to attach channel to " + tid + ":" + slot.name()
                                + ". Channel not found.");
                        }
                        return true;
                    }
                    case DETACH: {
                        LOG.info("Detach " + JsonUtils.printRequest(progress));
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
                        signalsQueue.forEach(s -> {
                            //noinspection ResultOfMethodCallIgnored
                            servant.control().signal(Tasks.TaskSignal.newBuilder().setSigValue(s.sig()).build());
                        });
                        return true;
                    }
                    case EXECUTESTOP: {
                        final ExecutionConcluded executeStop = progress.getExecuteStop();
                        LOG.info("Task " + tid + " exited rc: " + executeStop.getRc());
                        final boolean communicationNotCompleted = state != COMMUNICATION_COMPLETED;
                        if (executeStop.getRc() != 0) {
                            state(ERROR, executeStop.getRc(), "Exit code: " + executeStop.getRc(),
                                    executeStop.getDescription());
                        } else {
                            state(State.SUCCESS, 0, "Success");
                        }
                        servant = null;
                        TaskImpl.this.notifyAll();
                        return communicationNotCompleted;
                    }
                    case COMMUNICATIONCOMPLETED: {
                        if (state.phase() <= EXECUTING.phase()) {
                            state(COMMUNICATION_COMPLETED);
                            return true;
                        }
                        return false;
                    }
                    case DISCONNECTED: {
                        state(DISCONNECTED, 1, "Unexpected connection close");
                        return false;
                    }
                    default: {
                        return true;
                    }
                }
            }
        });
        Context.current().addListener((ctx) -> {
            synchronized (TaskImpl.this) {
                if (!EnumSet.of(ERROR, State.SUCCESS).contains(state)) {
                    state(State.DISCONNECTED);
                }
                servant = null;
                TaskImpl.this.notifyAll();
            }
        }, Runnable::run);
        this.servant = connection;
        TaskImpl.this.notifyAll();
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
        //noinspection ResultOfMethodCallIgnored
        connection.control().execute(taskSpecBuilder.build());
    }


    private void progress(Tasks.TaskProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    @Override
    public synchronized URI servantUri() {
        return servant != null ? servant.uri() : null;
    }

    @Override
    public Stream<Slot> slots() {
        return attachedSlots.keySet().stream();
    }

    @Override
    public SlotStatus slotStatus(Slot slot) throws TaskException {
        final LzyServantGrpc.LzyServantBlockingStub control;
        synchronized (this) {
            final Slot definedSlot = workload.slot(slot.name());
            if (servant == null) {
                if (definedSlot != null) {
                    return new PreparingSlotStatus(owner, this, definedSlot, assignments.get(slot));
                }
                throw new TaskException("No such slot: " + tid + ":" + slot);
            }
            control = servant.control();
        }
        final Servant.SlotCommandStatus slotStatus = control.configureSlot(
            Servant.SlotCommand.newBuilder()
                .setTid(tid.toString())
                .setSlot(slot.name())
                .setStatus(Servant.StatusCommand.newBuilder().build())
                .build()
        );
        return GrpcConverter.from(slotStatus.getStatus());
    }

    @Override
    public synchronized void signal(TasksManager.Signal signal) throws TaskException {
        if (EnumSet.of(ERROR, SUCCESS).contains(state()))
            throw new TaskException("Task is already concluded");
        signalsQueue.add(signal);
        if (servant != null) {
            LOG.info("Sending signal {} to servant {} for task {}", signal.name(), servant.uri(), tid);
            //noinspection ResultOfMethodCallIgnored
            servant.control().signal(Tasks.TaskSignal.newBuilder().setSigValue(signal.sig()).build());
        } else {
            LOG.info("Postponing signal {} for task {}", signal.name(), tid);
        }
    }
}
