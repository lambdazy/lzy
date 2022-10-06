package ai.lzy.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public class TimeUtils {

    public static boolean waitFlagUp(Supplier<Boolean> supplier, long timeout, TimeUnit unit) {
        boolean flag = false;
        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.MILLISECONDS.convert(timeout, unit)) {
            flag = supplier.get();
            if (flag) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
        return flag;
    }
}
