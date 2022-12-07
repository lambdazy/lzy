package ai.lzy.channelmanager.v2.debug;

import java.util.concurrent.atomic.AtomicInteger;

public class InjectedFailures {

    private static final int FAILURE_MARKERS_COUNT = 20;
    private static final AtomicInteger[] FAILURE_MARKERS = new AtomicInteger[FAILURE_MARKERS_COUNT];

    static {
        for (int i = 0; i < FAILURE_MARKERS_COUNT; ++i) {
            FAILURE_MARKERS[i] = new AtomicInteger(0);
        }
    }

    public static void setFailure(int markerIndex, int failOnTime) {
        assert markerIndex < FAILURE_MARKERS_COUNT;
        assert failOnTime > 0;
        FAILURE_MARKERS[markerIndex].set(failOnTime);
    }

    public static void fail0() {
        doFail(FAILURE_MARKERS[0]);
    }

    public static void fail1() {
        doFail(FAILURE_MARKERS[1]);
    }

    public static void fail2() {
        doFail(FAILURE_MARKERS[2]);
    }

    public static void fail3() {
        doFail(FAILURE_MARKERS[3]);
    }

    public static void fail4() {
        doFail(FAILURE_MARKERS[4]);
    }

    public static void fail5() {
        doFail(FAILURE_MARKERS[5]);
    }

    public static void fail6() {
        doFail(FAILURE_MARKERS[6]);
    }

    public static void fail7() {
        doFail(FAILURE_MARKERS[7]);
    }

    public static void fail8() {
        doFail(FAILURE_MARKERS[8]);
    }

    public static void fail9() {
        doFail(FAILURE_MARKERS[9]);
    }

    public static void fail10() {
        doFail(FAILURE_MARKERS[10]);
    }

    public static void fail11() {
        doFail(FAILURE_MARKERS[11]);
    }

    public static void fail12() {
        doFail(FAILURE_MARKERS[12]);
    }

    public static void fail13() {
        doFail(FAILURE_MARKERS[13]);
    }

    public static void fail14() {
        doFail(FAILURE_MARKERS[14]);
    }

    private static void doFail(AtomicInteger counter) {
        if (counter.get() == 0) {
            return;
        }
        if (counter.decrementAndGet() == 0) {
            throw new InjectedException();
        }
    }

    public static final class InjectedException extends RuntimeException {
    }

}
