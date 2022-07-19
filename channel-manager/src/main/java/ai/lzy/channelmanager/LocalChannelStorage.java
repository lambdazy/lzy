package ai.lzy.channelmanager;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelException;
import ai.lzy.channelmanager.channel.ChannelImpl;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.control.DirectChannelController;
import ai.lzy.channelmanager.control.SnapshotChannelController;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.channel.DirectChannelSpec;
import ai.lzy.model.channel.SnapshotChannelSpec;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.model.util.lock.LocalLockManager;
import ru.yandex.cloud.ml.platform.model.util.lock.LockManager;

public class LocalChannelStorage implements ChannelStorage {

    private final LockManager lockManager = new LocalLockManager();
    private final Map<String, Channel> channels = new ConcurrentHashMap<>();

    public LocalChannelStorage() {}

    @Override
    public Channel get(String channelId) {
        return channelId == null ? null : channels.get(channelId);
    }

    @Override
    public Channel create(ChannelSpec spec, String workflowId) {
        final String id = UUID.randomUUID().toString();
        final String name = spec.name() == null ? "unnamed_channel_" + UUID.randomUUID() : spec.name();
        if (spec instanceof DirectChannelSpec) {
            final Channel channel = new ChannelImpl(
                id,
                workflowId,
                spec,
                new DirectChannelController()
            );
            channels.put(id, channel);
            return channel;
        }
        if (spec instanceof SnapshotChannelSpec snapshotSpec) {
            final Channel channel = new SnapshotChannelImpl(
                name,
                workflowId,
                spec,
                snapshotSpec.snapshotId(),
                snapshotSpec.entryId(),
                snapshotSpec.getWhiteboardAddress()
            );
            channels.put(id, channel);
            return channel;
        }
        throw new RuntimeException("Wrong type of channel spec");
    }

    @Override
    public void destroy(String channelId) throws ChannelException {
        final Lock lock = lockManager.getOrCreate(channelId);
        lock.lock();
        try {
            final Channel channel = channels.remove(channelId);
            if (channel != null) {
                channel.close();
            } else {
                throw new ChannelException("Channel not found");
            }
        } finally {
            lock.unlock();
        }
    }

    @Nullable
    @Override
    public Channel bound(Endpoint endpoint) throws ChannelException {
        final List<Channel> boundChannels = channels.values()
            .stream()
            .filter(channel -> channel.hasBound(endpoint)).toList();
        if (boundChannels.size() == 0) {
            return null;
        }
        if (boundChannels.size() > 1) {
            throw new ChannelException(endpoint + " is bound to more than one channel");
        }
        return boundChannels.get(0);
    }

    @Override
    public Stream<Channel> channels() {
        return channels.values().stream();
    }

    private static class SnapshotChannelImpl extends ChannelImpl {
        SnapshotChannelImpl(String id, String workflowId, ChannelSpec channelSpec, String snapshotId, String entryId, URI whiteboardAddress) {
            super(id, workflowId, channelSpec, new SnapshotChannelController(entryId, snapshotId, whiteboardAddress));
        }
    }
}
