package ru.yandex.qe.s3.transfer;

import com.google.common.base.Throwables;
import java.util.function.Supplier;

/**
 * Established by terry on 10.08.15.
 */
@FunctionalInterface
public interface ThrowingSupplier<T> extends Supplier<T> {

    @Override
    default T get() {
        try {
            return getThrows();
        } catch (final Exception e) {
            throw Throwables.propagate(e);
        }
    }

    T getThrows() throws Exception;
}
