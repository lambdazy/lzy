package ru.yandex.cloud.ml.platform.lzy.server.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelEx;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.control.DirectChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.control.EmptyController;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import javax.annotation.Nullable;
import java.net.URI;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalChannelsRepository implements ChannelsRepository {
    private static final Logger LOG = LogManager.getLogger(InMemTasksManager.class);
    private final LockManager lockManager = new LocalLockManager();
    private final Map<String, ChannelEx> channels = new ConcurrentHashMap<>();
    private final Map<URI, ChannelEx> ibindings = new ConcurrentHashMap<>();
    private final Map<URI, Binding> bindings = new ConcurrentHashMap<>();

    @Override
    public Channel get(String cid) {
        return cid == null ? null : channels.get(cid);
    }

    @Override
    public Channel create(String name, DataSchema contentTypeFrom) {
        final Lock lock = lockManager.getOrCreate(name);
        lock.lock();
        try {
            if (channels.containsKey(name)) {
                return null;
            }
            final ChannelEx channel = new ChannelImpl(
                name == null ? UUID.randomUUID().toString() : name,
                contentTypeFrom
            );
            channels.put(channel.name(), channel);
            return channel;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void bind(Channel ch, Binding binding) throws ChannelException {
        final Lock lock = lockManager.getOrCreate(ch.name());
        lock.lock();
        try {
            final ChannelEx channel = ch instanceof ChannelEx ? (ChannelEx) ch : channels.get(ch.name());
            if (channel == null) {
                throw new ChannelException("Channel " + ch.name() + " is not registered");
            }
            if (ibindings.containsKey(binding.uri())) {
                if (channel.equals(ibindings.get(binding.uri()))) // already bound to this channel
                {
                    return;
                }
                throw new ChannelException("Bound to another channel: " + channel.name());
            }
            final Slot slot = binding.slot();
            switch (slot.direction()) { // type checking
                case INPUT:
                    //if (!slot.contentType().isAssignableFrom(channel.contentType())) {
                    //    throw new ChannelException(
                    //        "Channel content type " + channel.contentType() + " does not fit slot type " + slot
                    //            .contentType());
                    //}
                    break;
                case OUTPUT:
                    //if (!channel.contentType().isAssignableFrom(slot.contentType())) {
                    //    throw new ChannelException(
                    //        "Channel content type " + channel.contentType() + " does not fit slot type " + slot
                    //            .contentType());
                    //}
                    break;
            }

            ibindings.put(binding.uri(), channel);
            bindings.put(binding.uri(), binding);
            channel.bind(binding);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unbind(Channel ch, Binding binding) throws ChannelException {
        final Lock lock = lockManager.getOrCreate(ch.name());
        lock.lock();
        try {
            final ChannelEx channel = ch instanceof ChannelEx ? (ChannelEx) ch : channels.get(ch.name());
            ibindings.remove(binding.uri());
            bindings.remove(binding.uri());
            if (channel != null) {
                channel.unbind(binding);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void destroy(Channel ch) {
        final Lock lock = lockManager.getOrCreate(ch.name());
        lock.lock();
        try {
            ibindings.forEach((uri, channelEx) -> {
                if (channelEx.name().equals(ch.name())) {
                    unbind(ch, bindings.get(uri));
                }
            });
            final ChannelEx channel = channels.remove(ch.name());
            if (channel != null) {
                channel.close();
            }
        } finally {
            lock.unlock();
        }
    }

    @Nullable
    @Override
    public Channel bound(URI slot) {
        return ibindings.get(slot);
    }

    @Override
    public void unbindAll(URI servantUri) {
        LOG.info("LocalChannelsRepository::unbindAll " + servantUri);
        Binding.clearAll(servantUri);
        final String prefix = servantUri.toString();
        final Set<URI> unbind = ibindings.keySet()
            .stream()
            .filter(uri -> uri.toString().startsWith(prefix))
            .collect(Collectors.toSet());
        unbind.forEach(ub -> {
            try {
                unbind(ibindings.get(ub), bindings.get(ub));
            } catch (ChannelException ignore) {
            }
        });
    }

    @Override
    public SlotStatus[] connected(Channel ch) {
        final Lock lock = lockManager.getOrCreate(ch.name());
        lock.lock();
        try {
            final ChannelEx channel = ch instanceof ChannelEx ? (ChannelEx) ch : channels.get(ch.name());
            if (channel == null) {
                return new SlotStatus[0];
            }
            return channel.bound().map(s -> {
                final Servant.SlotCommandStatus slotCommandStatus = LzyServantGrpc.newBlockingStub(s.control())
                    .configureSlot(
                        Servant.SlotCommand.newBuilder()
                            .setSlot(s.slot().name())
                            .setStatus(Servant.StatusCommand.newBuilder().build())
                            .build()
                    );
                return gRPCConverter.from(slotCommandStatus.getStatus());
            }).toArray(SlotStatus[]::new);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Stream<Channel> channels() {
        return channels.values().stream().map(s -> s);
    }

    private static class ChannelImpl implements ChannelEx {
        private final String id;
        private final DataSchema contentType;
        private ChannelController logic; // pluggable channel logic
        private final Set<Binding> bound = new HashSet<>();

        ChannelImpl(String id, DataSchema contentType) {
            this.id = id;
            this.contentType = contentType;
            this.logic = new DirectChannelController(this);
        }

        @Override
        public String name() {
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
                bound.clear();
            } catch (ChannelException e) {
                LOG.warn("Exception during channel " + name() + " destruction", e);
            }
            logic = new EmptyController();
        }

        @Override
        public void bind(Binding binding) throws ChannelException {
            bound.add(binding);
            logic = logic.executeBind(binding);
        }

        @Override
        public void unbind(Binding binding) throws ChannelException {
            if (!bound.remove(binding)) {
                throw new ChannelException(MessageFormat.format(
                    "Slot {0} is not bound to the channel {1}",
                    binding.uri(), this.name()
                )
                );
            }
            logic = logic.executeUnBind(binding);
        }

        @Override
        public ChannelController controller() {
            return logic;
        }

        @Override
        public Stream<Binding> bound() {
            return Set.copyOf(bound).stream();
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
