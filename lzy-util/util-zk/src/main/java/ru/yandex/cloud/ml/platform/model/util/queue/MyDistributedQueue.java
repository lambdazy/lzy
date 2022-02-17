package ru.yandex.cloud.ml.platform.model.util.queue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.queue.ErrorMode;
import org.apache.curator.framework.recipes.queue.MultiItem;
import org.apache.curator.framework.recipes.queue.QueueBase;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueuePutListener;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyDistributedQueue<T> implements QueueBase<T> {

    private static final String QUEUE_ITEM_NAME = "queue-";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final QueueSerializer<T> serializer;
    private final String queuePath;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final String lockPath;
    private final AtomicReference<ErrorMode> errorMode = new AtomicReference<ErrorMode>(ErrorMode.REQUEUE);
    private final ListenerContainer<QueuePutListener<T>> putListenerContainer =
        new ListenerContainer<QueuePutListener<T>>();
    private final AtomicInteger lastChildCount = new AtomicInteger(0);
    private final int finalFlushMs = 5000;
    private final boolean putInBackground;
    private final MyChildrenCache childrenCache;

    private final AtomicInteger putCount = new AtomicInteger(0);
    private int maxItems;

    public MyDistributedQueue(
        CuratorFramework client,
        QueueSerializer<T> serializer,
        String queuePath,
        String lockPath,
        int maxItems,
        boolean putInBackground
    ) {
        Preconditions.checkNotNull(client, "client cannot be null");
        Preconditions.checkNotNull(serializer, "serializer cannot be null");
        Preconditions.checkArgument(maxItems > 0, "maxItems must be a positive number");

        this.lockPath = (lockPath == null) ? null : PathUtils.validatePath(lockPath);
        this.putInBackground = putInBackground;
        this.client = client;
        this.serializer = serializer;
        this.queuePath = PathUtils.validatePath(queuePath);
        this.maxItems = maxItems;
        childrenCache = new MyChildrenCache(client, queuePath);

        if (putInBackground) {
            log.warn(
                "Bounded queues should set putInBackground(false) in the builder."
                    + " Putting in the background will result in spotty maxItem consistency.");
        }
    }

    public void setMaxItems(int maxItems) {
        this.maxItems = maxItems;
    }

    /**
     * Start the queue. No other methods work until this is called
     *
     * @throws Exception startup errors
     */
    @Override
    public void start() throws Exception {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new IllegalStateException();
        }

        try {
            client.create().creatingParentContainersIfNeeded().forPath(queuePath);
        } catch (KeeperException.NodeExistsException ignore) {
            // this is OK
        }
        if (lockPath != null) {
            try {
                client.create().creatingParentContainersIfNeeded().forPath(lockPath);
            } catch (KeeperException.NodeExistsException ignore) {
                // this is OK
            }
        }

        childrenCache.start();
    }

    @Override
    public void close() throws IOException {
        if (state.compareAndSet(State.STARTED, State.STOPPED)) {
            if (finalFlushMs > 0) {
                try {
                    flushPuts(finalFlushMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            CloseableUtils.closeQuietly(childrenCache);
            putListenerContainer.clear();
        }
    }

    public T poll(int timeout, TimeUnit unit) throws InterruptedException {
        long currentVersion = -1;
        long maxWaitMs = -1;

        long timeoutMillis = unit.toMillis(timeout);
        try {
            while (state.get() == State.STARTED) {
                final long startTime = System.currentTimeMillis();
                MyChildrenCache.Data data = childrenCache.blockingNextGetData(
                    currentVersion,
                    timeoutMillis,
                    TimeUnit.MILLISECONDS
                );
                currentVersion = data.version;

                List<String> children = Lists.newArrayList(data.children);
                sortChildren(children); // makes sure items are processed in the correct order

                for (final String child : children) {
                    T result = processChildren(child, currentVersion);
                    if (result != null) {
                        return result;
                    }
                }
                long endTime = System.currentTimeMillis();
                timeoutMillis -= endTime - startTime;
                if (timeoutMillis < 0) {
                    log.info("Timeout polling pool item!");
                    return null;
                }
            }
        } catch (InterruptedException e) {
            log.info("Polling was cancelled", e);
            throw e;
        } catch (Exception e) {
            log.error("Exception caught in poll handler", e);
            throw new RuntimeException(e);
        }
        return null;
    }

    public int size() {
        try {
            return getChildren().size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the manager for put listeners
     *
     * @return put listener container
     */
    @Override
    public ListenerContainer<QueuePutListener<T>> getPutListenerContainer() {
        return putListenerContainer;
    }

    /**
     * Used when the queue is created with a {@link QueueBuilder#lockPath(String)}. Determines the behavior when the
     * queue consumer throws an exception
     *
     * @param newErrorMode the new error mode (the default is {@link ErrorMode#REQUEUE}
     */
    @Override
    public void setErrorMode(ErrorMode newErrorMode) {
        Preconditions.checkNotNull(lockPath, "lockPath cannot be null");

        if (newErrorMode == ErrorMode.REQUEUE) {
            log.warn(
                "ErrorMode.REQUEUE requires ZooKeeper version 3.4.x+ - make sure you are not using a prior version");
        }

        errorMode.set(newErrorMode);
    }

    /**
     * Wait until any pending puts are committed
     *
     * @param waitTime max wait time
     * @param timeUnit time unit
     * @return true if the flush was successful, false if it timed out first
     * @throws InterruptedException if thread was interrupted
     */
    @Override
    public boolean flushPuts(long waitTime, TimeUnit timeUnit) throws InterruptedException {
        long msWaitRemaining = TimeUnit.MILLISECONDS.convert(waitTime, timeUnit);
        synchronized (putCount) {
            while (putCount.get() > 0) {
                if (msWaitRemaining <= 0) {
                    return false;
                }

                long startMs = System.currentTimeMillis();

                putCount.wait(msWaitRemaining);

                long elapsedMs = System.currentTimeMillis() - startMs;
                msWaitRemaining -= elapsedMs;
            }
        }
        return true;
    }

    /**
     * Add an item into the queue. Adding is done in the background - thus, this method will return quickly.<br><br>
     * NOTE: if an upper bound was set via maxItems, this method will block until there is available space in the
     * queue.
     *
     * @param item item to add
     * @throws Exception connection issues
     */
    public void put(T item) throws Exception {
        put(item, 0, null);
    }

    /**
     * Same as {@link #put(Object)} but allows a maximum wait time if an upper bound was set via maxItems.
     *
     * @param item    item to add
     * @param maxWait maximum wait
     * @param unit    wait unit
     * @return true if items was added, false if timed out
     * @throws Exception .
     */
    public boolean put(T item, int maxWait, TimeUnit unit) throws Exception {
        checkState();

        String path = makeItemPath();
        return internalPut(item, null, path, maxWait, unit);
    }

    /**
     * Add a set of items into the queue. Adding is done in the background - thus, this method will return
     * quickly.<br><br> NOTE: if an upper bound was set via maxItems, this method will block until there is available
     * space in the queue.
     *
     * @param items items to add
     * @throws Exception connection issues
     */
    public void putMulti(MultiItem<T> items) throws Exception {
        putMulti(items, 0, null);
    }

    /**
     * Same as {@link #putMulti(MultiItem)} but allows a maximum wait time if an upper bound was set maxItems.
     *
     * @param items   items to add
     * @param maxWait maximum wait
     * @param unit    wait unit
     * @return true if items was added, false if timed out
     * @throws Exception .
     */
    public boolean putMulti(MultiItem<T> items, int maxWait, TimeUnit unit) throws Exception {
        checkState();

        String path = makeItemPath();
        return internalPut(null, items, path, maxWait, unit);
    }

    /**
     * Return the most recent message count from the queue. This is useful for debugging/information purposes only.
     *
     * @return count (can be 0)
     */
    @Override
    public int getLastMessageCount() {
        return lastChildCount.get();
    }

    private boolean internalPut(final T item, MultiItem<T> multiItem, String path, int maxWait, TimeUnit unit) throws
        Exception {
        if (!blockIfMaxed(maxWait, unit)) {
            return false;
        }

        final MultiItem<T> givenMultiItem = multiItem;
        if (item != null) {
            final AtomicReference<T> ref = new AtomicReference<T>(item);
            multiItem = () -> ref.getAndSet(null);
        }

        putCount.incrementAndGet();
        byte[] bytes = MyItemSerializer.serialize(multiItem, serializer);
        if (putInBackground) {
            doPutInBackground(item, path, givenMultiItem, bytes);
        } else {
            doPutInForeground(item, path, givenMultiItem, bytes);
        }
        return true;
    }

    private void doPutInForeground(final T item, String path, final MultiItem<T> givenMultiItem, byte[] bytes) throws
        Exception {
        client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, bytes);
        synchronized (putCount) {
            putCount.decrementAndGet();
            putCount.notifyAll();
        }
        putListenerContainer.forEach(
            listener -> {
                if (item != null) {
                    listener.putCompleted(item);
                } else {
                    listener.putMultiCompleted(givenMultiItem);
                }
                return null;
            }
        );
    }

    private void doPutInBackground(final T item, String path, final MultiItem<T> givenMultiItem, byte[] bytes) throws
        Exception {
        BackgroundCallback callback = (client, event) -> {
            if (event.getResultCode() != KeeperException.Code.OK.intValue()) {
                return;
            }

            if (event.getType() == CuratorEventType.CREATE) {
                synchronized (putCount) {
                    putCount.decrementAndGet();
                    putCount.notifyAll();
                }
            }

            putListenerContainer.forEach(
                new Function<QueuePutListener<T>, Void>() {
                    @Override
                    public Void apply(QueuePutListener<T> listener) {
                        if (item != null) {
                            listener.putCompleted(item);
                        } else {
                            listener.putMultiCompleted(givenMultiItem);
                        }
                        return null;
                    }
                }
            );
        };
        internalCreateNode(path, bytes, callback);
    }

    @VisibleForTesting
    void internalCreateNode(String path, byte[] bytes, BackgroundCallback callback) throws Exception {
        client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).inBackground(callback).forPath(path, bytes);
    }

    void checkState() throws Exception {
        if (state.get() != State.STARTED) {
            throw new IllegalStateException();
        }
    }

    String makeItemPath() {
        return ZKPaths.makePath(queuePath, QUEUE_ITEM_NAME);
    }

    @VisibleForTesting
    MyChildrenCache getCache() {
        return childrenCache;
    }

    protected void sortChildren(List<String> children) {
        Collections.sort(children);
    }

    protected List<String> getChildren() throws Exception {
        return client.getChildren().forPath(queuePath);
    }

    public List<T> toList() {
        try {
            return client.getChildren().forPath(queuePath).stream().map(child -> {
                try {
                    return processMessageBytes(child, client.getData().forPath(ZKPaths.makePath(queuePath, child)));
                } catch (KeeperException.NoNodeException ignored) {
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected long getDelay(String itemNode) {
        return 0;
    }

    private boolean blockIfMaxed(int maxWait, TimeUnit unit) throws Exception {
        MyChildrenCache.Data data = childrenCache.getData();
        while (data.children.size() >= maxItems) {
            long previousVersion = data.version;
            data = childrenCache.blockingNextGetData(data.version, maxWait, unit);
            if (data.version == previousVersion) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private T processChildren(String child, long currentVersion) throws Exception {
        final boolean isUsingLockSafety = (lockPath != null);

        if (!child.startsWith(QUEUE_ITEM_NAME)) {
            log.warn("Foreign node in queue path: " + child);
        }

        try {
            if (isUsingLockSafety) {
                return processWithLockSafety(child, ProcessType.NORMAL);
            } else {
                return processNormally(child, ProcessType.NORMAL);
            }
        } catch (Exception e) {
            log.warn("Failed to process children ", e);
            return null;
        }
    }

    private T processMessageBytes(String itemNode, byte[] bytes) throws Exception {
        MultiItem<T> items;
        try {
            items = MyItemSerializer.deserialize(bytes, serializer);
        } catch (Throwable e) {
            ThreadUtils.checkInterrupted(e);
            log.error("Corrupted queue item: " + itemNode, e);
            return null;
        }

        T item = items.nextItem();
        if (item == null) {
            return null;
        }

        try {
            return item;
        } catch (Throwable e) {
            ThreadUtils.checkInterrupted(e);
            log.error("Exception processing queue item: " + itemNode, e);
            return null;
        }
    }

    private T processNormally(String itemNode, ProcessType type) throws Exception {
        try {
            String itemPath = ZKPaths.makePath(queuePath, itemNode);
            Stat stat = new Stat();

            byte[] bytes = null;
            if (type == ProcessType.NORMAL) {
                bytes = client.getData().storingStatIn(stat).forPath(itemPath);
            }
            if (client.getState() == CuratorFrameworkState.STARTED) {
                client.delete().withVersion(stat.getVersion()).forPath(itemPath);
            }

            if (type == ProcessType.NORMAL) {
                return processMessageBytes(itemNode, bytes);
            }
        } catch (KeeperException.NodeExistsException ignore) {
            // another process got it
        } catch (KeeperException.NoNodeException ignore) {
            // another process got it
        } catch (KeeperException.BadVersionException ignore) {
            // another process got it
        }

        return null;
    }

    @VisibleForTesting
    protected T processWithLockSafety(String itemNode, ProcessType type) throws Exception {
        String lockNodePath = ZKPaths.makePath(lockPath, itemNode);
        boolean lockCreated = false;
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(lockNodePath);
            lockCreated = true;

            String itemPath = ZKPaths.makePath(queuePath, itemNode);
            byte[] bytes = null;
            T result = null;
            if (type == ProcessType.NORMAL) {
                bytes = client.getData().forPath(itemPath);
                result = processMessageBytes(itemNode, bytes);
            }

            if (result == null) {
                client.inTransaction()
                    .delete().forPath(itemPath)
                    .and()
                    .create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(makeRequeueItemPath(itemPath), bytes)
                    .and()
                    .commit();
            } else {
                client.delete().forPath(itemPath);
            }

            return result;
        } catch (KeeperException.NodeExistsException ignore) {
            // another process got it
        } catch (KeeperException.NoNodeException ignore) {
            // another process got it
        } catch (KeeperException.BadVersionException ignore) {
            // another process got it
        } finally {
            if (lockCreated) {
                client.delete().guaranteed().forPath(lockNodePath);
            }
        }

        return null;
    }

    protected String makeRequeueItemPath(String itemPath) {
        return makeItemPath();
    }

    private enum State {
        LATENT,
        STARTED,
        STOPPED
    }

    @VisibleForTesting
    protected enum ProcessType {
        NORMAL,
        REMOVE
    }

    private enum ProcessMessageBytesCode {
        NORMAL,
        REQUEUE
    }

}
