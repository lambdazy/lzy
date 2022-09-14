package ai.lzy.allocator.alloc.impl;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.allocator.model.Workload;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
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

    public static final String PORTAL_POOL_LABEL = "portals";

    private final Method vmMain;
    private final ConcurrentHashMap<String, Thread> vmThreads = new ConcurrentHashMap<>();
    private final ServiceConfig cfg;

    @Inject
    public ThreadVmAllocator(ServiceConfig serviceConfig, ServiceConfig.ThreadAllocator allocatorConfig) {
        cfg = serviceConfig;

        try {
            Class<?> vmClass;

            if (allocatorConfig.getVmJarFile() != null) {
                final File vmJar = new File(allocatorConfig.getVmJarFile());
                final URLClassLoader classLoader = new URLClassLoader(new URL[] {vmJar.toURI().toURL()},
                    ClassLoader.getSystemClassLoader());
                vmClass = Class.forName(allocatorConfig.getVmClassName(), true, classLoader);
            } else {
                vmClass = Class.forName(allocatorConfig.getVmClassName());
            }

            vmMain = vmClass.getDeclaredMethod("execute", String[].class);
        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void allocate(Vm.Spec vm) {
        allocateWithSingleWorkload(vm.vmId(), vm.poolLabel(), vm.workloads().get(0));
    }

    private void allocateWithSingleWorkload(String vmId, String poolLabel, Workload workload) {
        LOG.info("Allocating vm with id: " + vmId);

        var env = workload.env();
        if (!env.containsKey(AllocatorAgent.VM_ALLOCATOR_OTT)) {
            throw new AssertionError("Missing env " + AllocatorAgent.VM_ALLOCATOR_OTT);
        }

        var startupArgs = new ArrayList<>(workload.args());
        if (poolLabel.contentEquals(PORTAL_POOL_LABEL)) {
            startupArgs.addAll(List.of(
                "-portal.vm-id=" + vmId,
                "-portal.allocator-address=" + cfg.getAddress(),
                "-portal.allocator-heartbeat-period=" + cfg.getHeartbeatTimeout().dividedBy(2).toString(),
                "-portal.host=localhost",
                "-portal.allocator-token=" + env.get(AllocatorAgent.VM_ALLOCATOR_OTT)
            ));
            if (env.containsKey("LZY_PORTAL_PKEY")) {
                startupArgs.add("-portal.iam-token=" + env.get("LZY_PORTAL_PKEY"));
            }
        } else {
            startupArgs.addAll(List.of(
                "--vm-id", vmId,
                "--allocator-address", cfg.getAddress(),
                "--allocator-heartbeat-period", cfg.getHeartbeatTimeout().dividedBy(2).toString(),
                "--host", "localhost"
            ));
            startupArgs.add("--allocator-token");
            startupArgs.add('"' + env.get(AllocatorAgent.VM_ALLOCATOR_OTT) + '"');
            if (env.containsKey("LZY_WORKER_PKEY")) {
                startupArgs.add("--iam-token");
                startupArgs.add('"' + env.get("LZY_WORKER_PKEY") + '"');
            }
        }

        Thread vm = startThreadVm("vm-" + vmId, startupArgs);
        vmThreads.put(vmId, vm);
    }

    private Thread startThreadVm(String threadName, List<String> args) {
        @SuppressWarnings("CheckStyle")
        var task = new Thread(threadName) {
            @Override
            public void run() {
                try {
                    vmMain.invoke(null, (Object) args.toArray(new String[0]));
                } catch (InvocationTargetException e) {
                    LOG.error("Error while invocation of servant/portal method 'execute': " +
                        e.getTargetException().getMessage(), e.getTargetException());
                } catch (IllegalAccessException e) {
                    LOG.error("Error while invocation of servant/portal method 'execute': " + e.getMessage(), e);
                }
            }
        };

        LOG.debug("Starting thread-vm with name: " + threadName);

        task.start();
        return task;
    }

    @Override
    public void deallocate(String vmId) {
        LOG.info("Deallocate vm with id: " + vmId);

        if (!vmThreads.containsKey(vmId)) {
            return;
        }
        //noinspection removal
        vmThreads.get(vmId).stop();
        vmThreads.remove(vmId);
    }

    @Override
    public List<VmEndpoint> getVmEndpoints(String vmId, @Nullable TransactionHandle transaction) {
        return List.of(new VmEndpoint(VmEndpointType.HOST_NAME, "localhost"));
    }
}
