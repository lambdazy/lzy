package ai.lzy.allocator.model.debug;

import ai.lzy.allocator.model.Vm;
import lombok.Lombok;

import java.security.Permission;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class InjectedFailures {

    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_1 = new AtomicReference<>(null);
    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_2 = new AtomicReference<>(null);
    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_3 = new AtomicReference<>(null);
    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_4 = new AtomicReference<>(null);
    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_5 = new AtomicReference<>(null);
    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_6 = new AtomicReference<>(null);
    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_7 = new AtomicReference<>(null);
    public static final AtomicReference<Function<Vm, Throwable>> FAIL_ALLOCATE_VM_8 = new AtomicReference<>(null);

    public static void reset() {
        FAIL_ALLOCATE_VM_1.set(null);
        FAIL_ALLOCATE_VM_2.set(null);
        FAIL_ALLOCATE_VM_3.set(null);
        FAIL_ALLOCATE_VM_4.set(null);
        FAIL_ALLOCATE_VM_5.set(null);
        FAIL_ALLOCATE_VM_6.set(null);
        FAIL_ALLOCATE_VM_7.set(null);
        FAIL_ALLOCATE_VM_8.set(null);
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

    public static void failAllocateVm1(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_1, vm);
    }

    public static void failAllocateVm2(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_2, vm);
    }

    public static void failAllocateVm3(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_3, vm);
    }

    public static void failAllocateVm4(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_4, vm);
    }

    public static void failAllocateVm5(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_5, vm);
    }

    public static void failAllocateVm6(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_6, vm);
    }

    public static void failAllocateVm7(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_7, vm);
    }

    public static void failAllocateVm8(Vm vm) {
        failAllocateVmImpl(FAIL_ALLOCATE_VM_8, vm);
    }


    private static void failAllocateVmImpl(AtomicReference<Function<Vm, Throwable>> ref, Vm vm) {
        final var fn = ref.get();
        if (fn == null) {
            return;
        }
        final var th = fn.apply(vm);
        if (th != null) {
            ref.set(null);
            //throw Lombok.sneakyThrow(th);
            System.exit(42);
        }
    }

    public static final class TerminateException extends Exception {
        public TerminateException() {
        }

        public TerminateException(String message) {
            super(message);
        }
    }

}
