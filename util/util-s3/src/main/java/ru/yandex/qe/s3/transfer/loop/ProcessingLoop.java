package ru.yandex.qe.s3.transfer.loop;

import com.google.common.util.concurrent.*;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qe.s3.transfer.TransferAbortPolicy;
import ru.yandex.qe.s3.transfer.TransferStatistic;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Established by terry on 22.07.15.
 */
@NotThreadSafe
public abstract class ProcessingLoop<R> implements Callable<R> {

    protected static final long WAIT_QUANTUM = TimeUnit.MILLISECONDS.toMillis(100);
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingLoop.class);
    protected final List<ChunkTask> chunkTasks;
    private final ByteBufferPool byteBufferPool;
    private final ListeningExecutorService taskExecutor;
    private final TransferAbortPolicy abortPolicy;
    private final AtomicBoolean running;
    protected ByteBuffer loopThreadBuffer = null;

    private TransferStatistic currentStatistic;

    public ProcessingLoop(@Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService taskExecutor) {
        this(byteBufferPool, taskExecutor, TransferAbortPolicy.RETURN_IMMEDIATELY);
    }

    public ProcessingLoop(@Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService taskExecutor,
        @Nonnull TransferAbortPolicy abortPolicy) {
        this.byteBufferPool = byteBufferPool;
        this.taskExecutor = taskExecutor;
        this.chunkTasks = new CopyOnWriteArrayList<>();
        this.running = new AtomicBoolean();
        this.abortPolicy = abortPolicy;
    }

    @Override
    public R call() throws Exception {
        try {
            start();
            return callInner();
        } finally {
            stop();
            releaseAllBuffers();
        }
    }

    protected abstract R callInner() throws Exception;

    @Nullable
    protected ByteBuffer tryBorrowBuffer() throws Exception {
        try {
            return loopThreadBuffer = byteBufferPool.borrowObject(TimeUnit.MILLISECONDS.toMillis(WAIT_QUANTUM));
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    protected TransferStatistic getCurrentStatistic() {
        return currentStatistic;
    }

    protected synchronized TransferStatistic initCurrentStatistic(long contentLength) {
        return currentStatistic = new TransferStatistic(contentLength, chunkSize(),
            contentLength != TransferStatistic.UNDEFINED_LENGTH ? chunksCount(contentLength)
                : TransferStatistic.UNDEFINED_LENGTH, 0);
    }

    protected synchronized TransferStatistic incrementCurrentStatistic() {
        return currentStatistic = new TransferStatistic(currentStatistic.getObjectContentLength(),
            currentStatistic.getChunkSize(), currentStatistic.getExpectedChunksCount(),
            currentStatistic.getChunksTransferred() + 1);
    }

    protected long chunksCount(long contentLength) {
        long chunkSize = byteBufferPool.bufferSizeBytes();
        return contentLength / chunkSize + (contentLength % chunkSize > 0 ? 1 : 0);
    }

    protected long chunkSize() {
        return byteBufferPool.bufferSizeBytes();
    }

    protected int fill(ByteBuffer byteBuffer, InputStream inputStream) throws IOException {
        int readCount;
        do {
            readCount = inputStream.read(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining());

            byteBuffer.position(byteBuffer.position() + (readCount > 0 ? readCount : 0));
        } while (readCount != -1 && byteBuffer.hasRemaining());
        byteBuffer.flip().rewind();
        return byteBuffer.limit();
    }

    protected void processCompletedTasks() throws ExecutionException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
        for (ChunkTask chunkTask : chunkTasks) {
            if (chunkTask.isDone()) {
                chunkTask.get();
                byteBufferPool.returnObject(chunkTask.getBuffer());
                chunkTasks.remove(chunkTask);
            }
        }
        loopHeartbeat(getCurrentStatistic());
    }

    protected void waitAllTasks() throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
        waitAllTasks(Runnables.doNothing());
    }

    protected void waitAllTasks(Runnable stateChecker) throws Exception {
        while (!chunkTasks.isEmpty()) {
            stateChecker.run();
            loopHeartbeat(getCurrentStatistic());
            for (ChunkTask chunkTask : chunkTasks) {
                try {
                    chunkTask.get(WAIT_QUANTUM, TimeUnit.MILLISECONDS);
                    byteBufferPool.returnObject(chunkTask.getBuffer());
                    chunkTasks.remove(chunkTask);
                } catch (TimeoutException e) { //ignore
                }
            }
        }
    }

    protected void loopHeartbeat(TransferStatistic transferStatistic) {
    }

    @Nullable
    protected ChunkTask executeNewTask(ChunkRunnable chunkRunnable, FutureCallback<Void> callback) {
        if (!isRunning()) {
            return null;
        }

        final ChunkTask newTask = new ChunkTask(chunkRunnable, loopThreadBuffer, taskExecutor);
        chunkTasks.add(newTask);
        loopThreadBuffer = null; //buffer was distributed to chunk task
        Futures.addCallback(newTask, callback, MoreExecutors.directExecutor());
        return newTask;
    }

    protected boolean isNextChunkNumberToConsume(int chunkNumber) {
        final OptionalInt optionalMin = chunkTasks.stream().mapToInt(ChunkTask::getPartNumber).min();
        return !optionalMin.isPresent() || optionalMin.getAsInt() == chunkNumber;
    }

    private void start() {
        running.set(true);
    }

    protected boolean isRunning() {
        return running.get();
    }

    @Nonnull
    protected List<ChunkTask> stop() {
        running.set(false);

        final List<ChunkTask> canceled = new ArrayList<>();
        for (ChunkTask chunkTask : chunkTasks) {
            chunkTask.cancel(true);
            canceled.add(chunkTask);
        }
        return Collections.unmodifiableList(canceled);
    }

    protected void awaitTermination(@Nonnull List<? extends ChunkTask> chunkTasks) {
        if (abortPolicy == TransferAbortPolicy.RETURN_IMMEDIATELY) {
            return;
        }

        boolean wasInterrupted = false;
        LoggerFactory.getLogger(getClass()).debug("awaiting termination of chunk tasks: {}", chunkTasks.stream()
            .map(t -> '"' + t.toString() + '"')
            .collect(Collectors.joining(", ")));
        for (ChunkTask chunkTask : chunkTasks) {
            try {
                chunkTask.awaitTermination();
            } catch (CancellationException e) {
                LoggerFactory.getLogger(getClass())
                    .debug("chunk task \"{}\" was canceled before it got a chance to run", chunkTask);
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }
        }

        if (wasInterrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void releaseAllBuffers() {
        boolean skipThreadBuffer = false;
        for (ChunkTask task : chunkTasks) {
            releaseBufferOnTaskComplete(task);
            if (task.getBuffer() == loopThreadBuffer) {
                // prevent double release if loopThreadBuffer is not null and inside a task
                LOG.warn("loopThreadBuffer is not null but submitted inside a task");
                skipThreadBuffer = true;
            }
        }
        if (loopThreadBuffer != null && !skipThreadBuffer) {
            byteBufferPool.returnObject(loopThreadBuffer);
        }
    }

    private void releaseBufferOnTaskComplete(ChunkTask chunkTask) {
        while (true) {
            if (chunkTask.isDone() || chunkTask.isCancelled()) {
                byteBufferPool.returnObject(chunkTask.getBuffer());
                return;
            } else {
                Uninterruptibles.sleepUninterruptibly(WAIT_QUANTUM, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Nullable
    protected final InterruptedException interruptedException(@Nullable Throwable t) {
        return t instanceof InterruptedException
            ? (InterruptedException) t
            : (t == null ? null : interruptedException(t.getCause()));
    }

    private static final class UncheckedInterruptedException extends RuntimeException {

        private UncheckedInterruptedException(@Nonnull String message, @Nonnull InterruptedException cause) {
            super(message, cause);
        }
    }

    protected abstract class ChunkRunnable implements Runnable {

        private final CountDownLatch terminationLatch = new CountDownLatch(1);
        private final AtomicBoolean started = new AtomicBoolean();

        protected abstract void doRun();

        public abstract int getPartNumber();

        @Nonnull
        public abstract String getName();

        @Override
        public final void run() {
            try {
                checkCanceled();

                if (started.compareAndSet(false, true)) {
                    doRun();
                }
            } finally {
                terminationLatch.countDown();
            }
        }

        /**
         * @throws CancellationException task hasn't been started
         * @throws InterruptedException  current thread was interrupted while waiting for the task to terminate
         */
        public final void awaitTermination() throws CancellationException, InterruptedException {
            if (!started.get()) {
                throw new CancellationException("task \"" + getName() + "\" hasn't been started");
            }
            terminationLatch.await();
        }

        protected void checkCanceled() {
            if (Thread.currentThread().isInterrupted()) {
                throw new UncheckedInterruptedException("chunk task was interrupted", new InterruptedException());
            }

            if (!isRunning()) {
                LoggerFactory.getLogger(getClass())
                    .debug("processing loop has been stopped, but chunk task \"{}\" is still running", getName());
            }
        }
    }

    protected class ChunkTask implements ListenableFuture<Void> {

        private final ChunkRunnable runnable;
        private final ByteBuffer buffer;
        private final ListenableFuture<?> delegate;

        public ChunkTask(@Nonnull ChunkRunnable runnable, @Nonnull ByteBuffer buffer,
            @Nonnull ListeningExecutorService executor) {
            this.runnable = runnable;
            this.buffer = buffer;
            this.delegate = executor.submit(runnable);
        }

        @Nonnull
        public ByteBuffer getBuffer() {
            return buffer;
        }

        @Nonnull
        public String getName() {
            return runnable.getName();
        }

        public int getPartNumber() {
            return runnable.getPartNumber();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            delegate.get();
            return null;
        }

        @Override
        public Void get(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            delegate.get(timeout, unit);
            return null;
        }

        public void awaitTermination() throws InterruptedException {
            runnable.awaitTermination();
        }

        @Override
        public void addListener(@Nonnull Runnable listener, @Nonnull Executor executor) {
            delegate.addListener(listener, executor);
        }

        @Override
        public String toString() {
            return getName();
        }
    }
}
