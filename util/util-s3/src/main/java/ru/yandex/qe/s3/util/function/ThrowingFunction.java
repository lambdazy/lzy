package ru.yandex.qe.s3.util.function;

import com.google.common.base.Throwables;
import java.util.function.Function;

/**
 * Established by terry on 25.01.16.
 */
@FunctionalInterface
public interface ThrowingFunction<T, R> extends Function<T, R> {

    @Override
    default R apply(T arg) {
        try {
            return applyThrows(arg);
        } catch (final Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    R applyThrows(T arg) throws Exception;
}
