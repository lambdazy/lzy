package ai.lzy.channelmanager.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class InjectedFailures {

    private static final int FAILURE_MARKERS_COUNT = 20;
    private static final Marker[] FAILURE_MARKERS = new Marker[FAILURE_MARKERS_COUNT];

    static {
        clear();
    }

    public static void clear() {
        for (int i = 0; i < FAILURE_MARKERS_COUNT; ++i) {
            FAILURE_MARKERS[i] = new Marker();
        }
    }

    public static void assertClean() {
        for (int i = 0; i < FAILURE_MARKERS_COUNT; ++i) {
            assert FAILURE_MARKERS[i].get() == 0 : "Marker " + i + " isn't clean";
        }
    }

    public static void setFailure(int markerIndex, int failOnTime) {
        assert markerIndex < FAILURE_MARKERS_COUNT;
        assert failOnTime > 0;
        FAILURE_MARKERS[markerIndex].set(failOnTime);
    }

    public static boolean awaitFailure(int markerIndex) {
        assert markerIndex < FAILURE_MARKERS_COUNT;
        return FAILURE_MARKERS[markerIndex].await();
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

    private static void doFail(Marker marker) {
        if (marker.get() == 0) {
            return;
        }
        if (marker.decrementAndGet() == 0) {
            throw new InjectedException();
        }
    }

    public static final class InjectedException extends RuntimeException {
        public InjectedException() {
            super("Injected");
        }
    }

    private static class Marker {
        private CountDownLatch counter = null;

        private void set(int x) {
            synchronized (this) {
                counter = new CountDownLatch(x);
            }
        }

        private int get() {
            synchronized (this) {
                if (counter == null) {
                    return 0;
                }
                return (int) counter.getCount();
            }
        }

        private int decrementAndGet() {
            synchronized (this) {
                if (counter == null) {
                    return 0;
                }
                counter.countDown();
                return (int) counter.getCount();
            }
        }

        private boolean await() {
            if (counter == null) {
                return false;
            }
            try {
                return counter.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }

    }

}
