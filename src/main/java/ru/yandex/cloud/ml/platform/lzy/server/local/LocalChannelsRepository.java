package ru.yandex.cloud.ml.platform.lzy.server.local;

import org.apache.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelEx;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.control.DirectChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.control.EmptyController;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import javax.annotation.Nullable;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class LocalChannelsRepository implements ChannelsRepository {
    private static final Logger LOG = Logger.getLogger(LocalTasksManager.class);
    private final Map<UUID, ChannelEx> channels = new HashMap<>();
    private final Map<Binding, ChannelEx> ibindings = new HashMap<>();

    @Override
    public Channel get(UUID cid) {
        return channels.get(cid);
    }

    @Override
    public Channel create(DataSchema contentTypeFrom) {
        final ChannelEx channel = new ChannelImpl(UUID.randomUUID(), contentTypeFrom);
        channels.put(channel.id(), channel);
        return channel;
    }

    @Override
    public void bind(Channel ch, Task task, Slot slot) throws ChannelException {
        final ChannelEx channel = ch instanceof ChannelEx ? (ChannelEx) ch : channels.get(ch.id());
        if (channel == null) {
            throw new ChannelException("Channel " + ch.id() + " is not registered");
        }
        final Binding binding = new Binding(task, slot);
        if (ibindings.containsKey(binding)) {
            if (channel.equals(ibindings.get(binding))) // already bound to this channel
                return;
            throw new ChannelException("Bound to another channel: " + channel.id());
        }
        switch (slot.direction()) { // type checking
            case INPUT:
                if (!slot.contentType().isAssignableFrom(channel.contentType())) {
                    throw new ChannelException(
                        "Channel content type " + channel.contentType() + " does not fit slot type " + slot
                            .contentType());
                }
                break;
            case OUTPUT:
                if (!channel.contentType().isAssignableFrom(slot.contentType())) {
                    throw new ChannelException(
                        "Channel content type " + channel.contentType() + " does not fit slot type " + slot
                            .contentType());
                }
                break;
        }

        ibindings.put(binding, channel);
        channel.bind(task, slot);
    }

    @Override
    public void unbind(Channel ch, Task servant, Slot slot) throws ChannelException {
        final ChannelEx channel = ch instanceof ChannelEx ? (ChannelEx) ch : channels.get(ch.id());
        if (channel != null) channel.unbind(servant, slot);
    }

    @Override
    public void destroy(Channel ch) {
        final ChannelEx channel = ch instanceof ChannelEx ? (ChannelEx) ch : channels.get(ch.id());
        if (channel != null) channel.close();
    }

    @Nullable
    @Override
    public Channel bound(Task servant, Slot slot) {
        return ibindings.get(new Binding(servant, slot));
    }

    @Override
    public SlotStatus[] connected(Channel ch) {
        final ChannelEx channel = ch instanceof ChannelEx ? (ChannelEx) ch : channels.get(ch.id());
        if (channel == null)
            return new SlotStatus[0];
        return channel.bound().map(s -> s.task().slotStatus(s.slot().name())).toArray(SlotStatus[]::new);
    }

    @Override
    public Stream<Channel> channels() {
        return channels.values().stream().map(s -> s);
    }

    public static class ChannelImpl implements ChannelEx {
        private final UUID id;
        private final DataSchema contentType;
        private ChannelController logic; // pluggable channel logic
        private Map<Task, Slot> bound = new HashMap<>();

        ChannelImpl(UUID id, DataSchema contentType) {
            this.id = id;
            this.contentType = contentType;
            this.logic = new DirectChannelController(this);
        }

        @Override
        public UUID id() {
            return id;
        }

        @Override
        public DataSchema contentType() {
            return contentType;
        }

        @Override
        public void close() {
            try {
                logic.executeDestroy();
            }
            catch (ChannelException e) {
                LOG.warn("Exception during channel " + id() + " destruction", e);
            }
            logic = new EmptyController();
        }

        @Override
        public void bind(Task task, Slot slot) throws ChannelException {
            logic = logic.executeBind(new Binding(task, slot));
            bound.put(task, slot);
        }

        @Override
        public void unbind(Task task, Slot slot) throws ChannelException {
            if (bound.remove(task) == null) {
                throw new ChannelException(MessageFormat.format(
                    "Slot {0}:{1} is not bound to the channel {2}",
                    task.tid(), slot.name(), this.id()));
            }
            logic = logic.executeUnBind(new Binding(task, slot));
        }

        @Override
        public ChannelController controller() {
            return logic;
        }

        @Override
        public Stream<Binding> bound() {
            return bound.entrySet().stream().map(e -> new Binding(e.getKey(), e.getValue()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ChannelImpl channel = (ChannelImpl) o;
            return id.equals(channel.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
