package ai.lzy.allocator.model.debug;

import lombok.Lombok;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class InjectedFailures {

    public static final List<AtomicReference<Runnable>> FAIL_ALLOCATE_VMS = List.of(
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null)
    );

    public static final List<AtomicReference<Supplier<Throwable>>> FAIL_CREATE_DISK = List.of(
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null)
    );

    public static final List<AtomicReference<Supplier<Throwable>>> FAIL_DELETE_DISK = List.of(
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null)
    );

    public static final List<AtomicReference<Supplier<Throwable>>> FAIL_CLONE_DISK = List.of(
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null),
        new AtomicReference<>(null), new AtomicReference<>(null), new AtomicReference<>(null)
    );


    public static void reset() {
        FAIL_ALLOCATE_VMS.forEach(x -> x.set(null));
        FAIL_CREATE_DISK.forEach(x -> x.set(null));
        FAIL_DELETE_DISK.forEach(x -> x.set(null));
        FAIL_CLONE_DISK.forEach(x -> x.set(null));
    }

    public static void failAllocateVm(int n) {
        failImpl0(FAIL_ALLOCATE_VMS.get(n));
    }

    public static void failAllocateVm0() {
        failAllocateVm(0);
    }

    public static void failAllocateVm1() {
        failAllocateVm(1);
    }

    public static void failAllocateVm2() {
        failAllocateVm(2);
    }

    public static void failAllocateVm3() {
        failAllocateVm(3);
    }

    public static void failAllocateVm4() {
        failAllocateVm(4);
    }

    public static void failAllocateVm5() {
        failAllocateVm(5);
    }

    public static void failAllocateVm6() {
        failAllocateVm(6);
    }

    public static void failAllocateVm7() {
        failAllocateVm(7);
    }

    public static void failAllocateVm8() {
        failAllocateVm(8);
    }

    public static void failAllocateVm9() {
        failAllocateVm(9);
    }

    public static void failAllocateVm10() {
        failAllocateVm(10);
    }

    public static void failCreateDisk(int n) {
        failImpl(FAIL_CREATE_DISK.get(n));
    }

    public static void failCreateDisk0() {
        failCreateDisk(0);
    }

    public static void failCreateDisk1() {
        failCreateDisk(1);
    }

    public static void failCreateDisk2() {
        failCreateDisk(2);
    }

    public static void failDeleteDisk(int n) {
        failImpl(FAIL_DELETE_DISK.get(n));
    }

    public static void failDeleteDisk0() {
        failDeleteDisk(0);
    }

    public static void failDeleteDisk1() {
        failDeleteDisk(1);
    }

    public static void failDeleteDisk2() {
        failDeleteDisk(2);
    }

    public static void failCloneDisk(int n) {
        failImpl(FAIL_CLONE_DISK.get(n));
    }

    public static void failCloneDisk0() {
        failCloneDisk(0);
    }

    public static void failCloneDisk1() {
        failCloneDisk(1);
    }

    public static void failCloneDisk2() {
        failCloneDisk(2);
    }

    public static void failCloneDisk3() {
        failCloneDisk(3);
    }

    private static void failImpl(AtomicReference<Supplier<Throwable>> ref) {
        final var fn = ref.get();
        if (fn == null) {
            return;
        }
        final var th = fn.get();
        if (th != null) {
            ref.set(null);
            throw Lombok.sneakyThrow(th);
        }
    }

    private static void failImpl0(AtomicReference<Runnable> ref) {
        final var fn = ref.get();
        if (fn == null) {
            return;
        }
        ref.set(null);
        fn.run();
    }

    public static final class TerminateException extends Error {
        public TerminateException() {
        }

        public TerminateException(String message) {
            super(message);
        }
    }
}
