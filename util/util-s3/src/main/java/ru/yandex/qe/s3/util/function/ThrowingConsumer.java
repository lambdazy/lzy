package ru.yandex.qe.s3.util.function;

import com.google.common.base.Throwables;
import java.util.function.Consumer;

/**
 * Established by terry on 25.01.16.
 */
@FunctionalInterface
public interface ThrowingConsumer<T> extends Consumer<T> {

    @Override
    default void accept(final T arg) {
        try {
            acceptThrows(arg);
        } catch (final Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    void acceptThrows(T arg) throws Exception;
}
