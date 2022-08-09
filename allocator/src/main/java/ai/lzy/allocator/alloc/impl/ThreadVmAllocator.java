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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
@Requires(property = "allocator.thread-allocator.enabled", value = "true")
public class ThreadVmAllocator implements VmAllocator {
    private static final Logger LOG = LogManager.getLogger(ThreadVmAllocator.class);

    private final Method vmMain;
    private final AtomicInteger vmCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Thread> vmThreads = new ConcurrentHashMap<>();

    @Inject
    public ThreadVmAllocator(ServiceConfig serviceConfig) {
        try {
            Class<?> vmClass;

            if (!serviceConfig.threadAllocator().vmJarFile().isEmpty()) {
                final File vmJar = new File(serviceConfig.threadAllocator().vmJarFile());
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{vmJar.toURI().toURL()},
                    ClassLoader.getSystemClassLoader());
                vmClass = Class.forName(serviceConfig.threadAllocator().vmClassName(), true, classLoader);
            } else {
                vmClass = Class.forName("ai.lzy.servant.BashApi");
            }

            vmMain = vmClass.getDeclaredMethod("execute", String[].class);
        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private void requestAllocation(String vmId, List<String> args) {
        int servantNumber = vmCounter.incrementAndGet();
        LOG.info("Allocating vm {}", vmId);
        int port = FreePortFinder.find(10000, 11000);

        @SuppressWarnings("CheckStyle")
        Thread task = new Thread("vm-" + vmId) {
            @Override
            public void run() {
                try {
                    vmMain.invoke(null, (Object) args.toArray(new String[0]));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.error(e);
                }
            }
        };
        task.start();
        vmThreads.put(vmId, task);
    }

    @Override
    public Map<String, String> allocate(Vm vm) {
        // TODO(artolord) add token
        requestAllocation(vm.vmId(), vm.workloads().get(0).args());  // Supports only one workload
        return new HashMap<>();
    }

    @Override
    public void deallocate(Vm vm) {
        if (!vmThreads.containsKey(vm.vmId())) {
            return;
        }
        vmThreads.get(vm.vmId()).stop();
        vmThreads.remove(vm.vmId());
    }
}
