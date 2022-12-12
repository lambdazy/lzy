package ai.lzy.allocator.model.debug;

import ai.lzy.allocator.model.Vm;
import lombok.Lombok;

import java.security.Permission;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class InjectedFailures {

    public static final List<AtomicReference<Function<Vm, Throwable>>> FAIL_ALLOCATE_VMS = List.of(
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

    public static SecurityManager prepareForTests() {
        var sm = System.getSecurityManager();

        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {
            }

            @Override
            public void checkPermission(Permission perm, Object context) {
            }

            @Override
            public void checkExit(int status) {
                if (status == 42) {
                    throw Lombok.sneakyThrow(new TerminateException());
                }
            }
        });

        return sm;
    }

    public static void failAllocateVm0(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(0), vm);
    }

    public static void failAllocateVm1(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(1), vm);
    }

    public static void failAllocateVm2(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(2), vm);
    }

    public static void failAllocateVm3(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(3), vm);
    }

    public static void failAllocateVm4(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(4), vm);
    }

    public static void failAllocateVm5(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(5), vm);
    }

    public static void failAllocateVm6(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(6), vm);
    }

    public static void failAllocateVm7(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(7), vm);
    }

    public static void failAllocateVm8(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VMS.get(8), vm);
    }

    public static void failCreateDisk0() {
        failDiskOpImpl(FAIL_CREATE_DISK.get(0));
    }

    public static void failCreateDisk1() {
        failDiskOpImpl(FAIL_CREATE_DISK.get(1));
    }

    public static void failCreateDisk2() {
        failDiskOpImpl(FAIL_CREATE_DISK.get(2));
    }

    public static void failDeleteDisk0() {
        failDiskOpImpl(FAIL_DELETE_DISK.get(0));
    }

    public static void failDeleteDisk1() {
        failDiskOpImpl(FAIL_DELETE_DISK.get(1));
    }

    public static void failDeleteDisk2() {
        failDiskOpImpl(FAIL_DELETE_DISK.get(2));
    }

    public static void failCloneDisk0() {
        failDiskOpImpl(FAIL_CLONE_DISK.get(0));
    }

    public static void failCloneDisk1() {
        failDiskOpImpl(FAIL_CLONE_DISK.get(1));
    }

    public static void failCloneDisk2() {
        failDiskOpImpl(FAIL_CLONE_DISK.get(2));
    }

    public static void failCloneDisk3() {
        failDiskOpImpl(FAIL_CLONE_DISK.get(3));
    }


    private static void failAllocateVmImpl(AtomicReference<Function<Vm, Throwable>> ref, Vm vm) {
        final var fn = ref.get();
        if (fn == null) {
            return;
        }
        final var th = fn.apply(vm);
        if (th != null) {
            ref.set(null);
            if (th instanceof TerminateProcess) {
                Runtime.getRuntime().halt(42);
                // System.exit(42);
            }
            throw Lombok.sneakyThrow(th);
        }
    }

    private static void failDiskOpImpl(AtomicReference<Supplier<Throwable>> ref) {
        final var fn = ref.get();
        if (fn == null) {
            return;
        }
        final var th = fn.get();
        if (th != null) {
            ref.set(null);
            if (th instanceof TerminateProcess) {
                Runtime.getRuntime().halt(42);
                // System.exit(42);
            }
            throw Lombok.sneakyThrow(th);
        }
    }

    public static final class TerminateProcess extends RuntimeException {
    }

    public static final class TerminateException extends Exception {
        public TerminateException() {
        }

        public TerminateException(String message) {
            super(message);
        }
    }

}
