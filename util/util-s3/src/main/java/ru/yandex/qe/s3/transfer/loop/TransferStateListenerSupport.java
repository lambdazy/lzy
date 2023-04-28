package ru.yandex.qe.s3.transfer.loop;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import ru.yandex.qe.s3.transfer.TransferState;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author entropia
 */
public final class TransferStateListenerSupport<T extends TransferState> {

    private final Executor notifyExecutor;
    private final Consumer<T> progressListener;
    private final AtomicBoolean deliver;

    public TransferStateListenerSupport(@Nullable Executor notifyExecutor, @Nullable Consumer<T> progressListener) {
        this.notifyExecutor = notifyExecutor;
        this.progressListener = progressListener;
        this.deliver = new AtomicBoolean(true);
    }

    public void notifyListener(@Nonnull Supplier<T> newStateSupplier) {
        if (notifyExecutor != null && progressListener != null) {
            notifyListener0(newStateSupplier.get(), notifyExecutor, progressListener);
        }
    }

    public void notifyListener(@Nonnull T newState) {
        if (notifyExecutor != null && progressListener != null) {
            notifyListener0(newState, notifyExecutor, progressListener);
        }
    }

    private void notifyListener0(@Nonnull T newState, @Nonnull Executor executor, @Nonnull Consumer<T> listener) {
        final boolean finalNotification = newState.isFinal();

        if (!deliver.get()) {
            // don't deliver notifications if a notification with final transfer status has already been enqueued
            return;
        }

        if (finalNotification && !deliver.compareAndSet(true, false)) {
            // ignore duplicate notifications with final transfer status
            return;
        }

        executor.execute(() -> listener.accept(newState));
    }
}
