package ru.yandex.cloud.ml.platform.lzy.server.local;

import jakarta.inject.Singleton;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Slot.Direction;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelEx;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelGraph;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import ru.yandex.cloud.ml.platform.lzy.server.channel.control.DirectChannelController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.control.EmptyController;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;

@Singleton
public class LocalChannelsManager implements ChannelsManager {

    private static final Logger LOG = LogManager.getLogger(LocalChannelsManager.class);
    private final LockManager lockManager = new LocalLockManager();
    private final Map<String, ChannelEx> channels = new ConcurrentHashMap<>();

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

    private ChannelEx getBoundChannel(Endpoint endpoint) {
        for (ChannelEx channel : channels.values()) {
            if (channel.hasBound(endpoint)) {
                return channel;
            }
        }
        return null;
    }

    @Override
    public void bind(Channel ch, Endpoint endpoint) throws ChannelException {
        final Lock lock = lockManager.getOrCreate(ch.name());
        lock.lock();
        try {
            final ChannelEx channel = channels.get(ch.name());
            if (channel == null) {
                throw new ChannelException("Channel " + ch.name() + " is not registered");
            }
            final ChannelEx boundChannel = getBoundChannel(endpoint);
            if (boundChannel != null && !boundChannel.equals(ch)) {
                throw new ChannelException(
                    "Endpoint " + endpoint + " bound to another channel: " + channel.name());
            }
            final Slot slot = endpoint.slot();
            switch (slot.direction()) { // type checking
                case INPUT:
                    // if (!slot.contentType().isAssignableFrom(channel.contentType())) {
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
                default:
                    throw new IllegalStateException("Unexpected value: " + slot.direction());
            }
            channel.bind(endpoint);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unbind(Channel ch, Endpoint endpoint) throws ChannelException {
        final Lock lock = lockManager.getOrCreate(ch.name());
        lock.lock();
        try {
            final ChannelEx channel = channels.get(ch.name());
            if (channel != null) {
                channel.unbind(endpoint);
            } else {
                LOG.warn("Attempt to unbind endpoint " + endpoint + " from unregistered channel "
                    + ch.name());
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
    public Channel bound(Endpoint endpoint) {
        final List<ChannelEx> boundChannels = channels.values()
            .stream()
            .filter(channel -> channel.hasBound(endpoint))
            .collect(Collectors.toList());
        if (boundChannels.size() == 0) {
            return null;
        }
        if (boundChannels.size() > 1) {
            throw new ChannelException(endpoint + " is bound to more than one channel");
        }
        return boundChannels.get(0);
    }

    @Override
    public void unbindAll(UUID sessionId) {
        LOG.info("LocalChannelsRepository::unbindAll sessionId=" + sessionId);
        for (ChannelEx channel : channels.values()) {
            final Lock lock = lockManager.getOrCreate(channel.name());
            lock.lock();
            try {
                //unbind receivers
                channel
                    .bound()
                    .filter(endpoint -> endpoint.sessionId().equals(sessionId))
                    .filter(endpoint -> endpoint.slot().direction() == Direction.INPUT)
                    .forEach(endpoint -> {
                        try {
                            channel.unbind(endpoint);
                        } catch (ChannelException e) {
                            LOG.warn("Fail to unbind " + endpoint + " from channel " + channel);
                        }
                    });
                //unbind senders
                channel
                    .bound()
                    .filter(endpoint -> endpoint.sessionId().equals(sessionId))
                    .filter(endpoint -> endpoint.slot().direction() == Direction.OUTPUT)
                    .forEach(endpoint -> {
                        try {
                            channel.unbind(endpoint);
                        } catch (ChannelException e) {
                            LOG.warn("Fail to unbind " + endpoint + " from channel " + channel);
                        }
                    });
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public SlotStatus[] connected(Channel ch) {
        final Lock lock = lockManager.getOrCreate(ch.name());
        lock.lock();
        try {
            final ChannelEx channel = channels.get(ch.name());
            if (channel == null) {
                return new SlotStatus[0];
            }
            return channel.bound().map(Endpoint::status).filter(Objects::nonNull)
                .toArray(SlotStatus[]::new);
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
        private final ChannelGraph channelGraph;
        private ChannelController logic; // pluggable channel logic

        ChannelImpl(String id, DataSchema contentType) {
            this.id = id;
            this.contentType = contentType;
            this.logic = new DirectChannelController();
            this.channelGraph = new LocalChannelGraph();
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
                logic.executeDestroy(channelGraph);
            } catch (ChannelException e) {
                LOG.warn("Exception during channel " + name() + " destruction", e);
            }
            logic = new EmptyController();
        }

        @Override
        public void bind(Endpoint endpoint) throws ChannelException {
            logic.executeBind(channelGraph, endpoint);
        }

        @Override
        public void unbind(Endpoint endpoint) throws ChannelException {
            if (!channelGraph.hasBound(endpoint)) {
                LOG.warn(MessageFormat.format(
                    "Slot {0} is not bound to the channel {1}",
                    endpoint.uri(), name()
                ));
                return;
            }
            logic.executeUnBind(channelGraph, endpoint);
        }

        @Override
        public ChannelController controller() {
            return logic;
        }

        @Override
        public Stream<Endpoint> bound() {
            return Stream.concat(
                channelGraph.senders().stream(),
                channelGraph.receivers().stream()
            );
        }

        @Override
        public boolean hasBound(Endpoint endpoint) {
            return channelGraph.hasBound(endpoint);
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
