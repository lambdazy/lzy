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
import java.util.ArrayList;
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
    private final Map<String, List<Channel>> channelsByWorkflow = new ConcurrentHashMap<>();

    public LocalChannelStorage() {
    }

    @Override
    public Channel get(String channelId) {
        return channelId == null ? null : channels.get(channelId);
    }

    @Override
    public Channel create(ChannelSpec spec, String workflowId, String userId) {
        final String id = spec.name() == null ? "unnamed_channel_" + UUID.randomUUID() : spec.name();
        final Channel channel;
        if (spec instanceof DirectChannelSpec) {
            channel = new ChannelImpl(
                id,
                spec,
                new DirectChannelController()
            );
        } else if (spec instanceof SnapshotChannelSpec snapshotSpec) {
            channel = new SnapshotChannelImpl(
                id,
                spec,
                snapshotSpec.snapshotId(),
                snapshotSpec.entryId(),
                userId,
                snapshotSpec.getWhiteboardAddress()
            );
        } else {
            throw new RuntimeException("Wrong type of channel spec");
        }
        channels.put(id, channel);
        channelsByWorkflow.putIfAbsent(workflowId, new ArrayList<>());
        channelsByWorkflow.get(workflowId).add(channel);
        return channel;
    }

    @Override
    public void destroy(String channelId) {
        final Lock lock = lockManager.getOrCreate(channelId);
        lock.lock();
        try {
            final Channel channel = channels.remove(channelId);
            if (channel != null) {
                channel.close();
            }
            channelsByWorkflow.values().forEach(channels -> channels.remove(channel));
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

    @Override
    public Stream<Channel> channels(String workflowId) {
        final List<Channel> channels = channelsByWorkflow.get(workflowId);
        if (channels != null) {
            return new ArrayList<>(channels).stream();
        }
        return Stream.empty();
    }

    private static class SnapshotChannelImpl extends ChannelImpl {

        SnapshotChannelImpl(
            String id,
            ChannelSpec channelSpec,
            String snapshotId,
            String entryId,
            String userId,
            URI whiteboardAddress
        ) {
            super(
                id,
                channelSpec,
                new SnapshotChannelController(entryId, snapshotId, userId, whiteboardAddress));
        }
    }
}
