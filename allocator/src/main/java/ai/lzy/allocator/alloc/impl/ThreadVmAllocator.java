package ai.lzy.allocator.alloc.impl;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@Requires(property = "allocator.thread-allocator.enabled", value = "true")
public class ThreadVmAllocator implements VmAllocator {
    private static final Logger LOG = LogManager.getLogger(ThreadVmAllocator.class);

    private final Method vmMain;
    private final ConcurrentHashMap<String, Thread> vmThreads = new ConcurrentHashMap<>();
    private final ServiceConfig cfg;

    @Inject
    public ThreadVmAllocator(ServiceConfig serviceConfig, ServiceConfig.ThreadAllocator allocatorConfig) {
        cfg = serviceConfig;

        try {
            Class<?> vmClass;

            if (!allocatorConfig.getVmJarFile().isEmpty()) {
                final File vmJar = new File(allocatorConfig.getVmJarFile());
                final URLClassLoader classLoader = new URLClassLoader(new URL[] {vmJar.toURI().toURL()},
                    ClassLoader.getSystemClassLoader());
                vmClass = Class.forName(allocatorConfig.getVmClassName(), true, classLoader);
            } else {
                vmClass = Class.forName("ai.lzy.servant.BashApi");
            }

            vmMain = vmClass.getDeclaredMethod("execute", String[].class);
        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private void requestAllocation(String vmId, List<String> args) {
        LOG.info("Allocating vm {}", vmId);
        final var newArgs = new ArrayList<>(args);
        newArgs.addAll(List.of(
            "--vm-id", vmId,
            "--allocator-address", cfg.getAddress(),
            "--allocator-heartbeat-period", cfg.getHeartbeatTimeout().dividedBy(2).toString()
        ));

        @SuppressWarnings("CheckStyle")
        Thread task = new Thread("vm-" + vmId) {
            @Override
            public void run() {
                try {
                    vmMain.invoke(null, (Object) newArgs.toArray(new String[0]));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.error(e);
                }
            }
        };
        task.start();
        vmThreads.put(vmId, task);
    }

    @Override
    public void allocate(Vm vm) {
        requestAllocation(vm.vmId(), vm.workloads().get(0).args());  // Supports only one workload
    }

    @Override
    public void deallocate(Vm vm) {
        if (!vmThreads.containsKey(vm.vmId())) {
            return;
        }
        //noinspection removal
        vmThreads.get(vm.vmId()).stop();
        vmThreads.remove(vm.vmId());
    }
}
