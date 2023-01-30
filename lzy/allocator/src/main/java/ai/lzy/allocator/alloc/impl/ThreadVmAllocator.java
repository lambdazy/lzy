package ai.lzy.allocator.alloc.impl;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.model.db.TransactionHandle;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

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
                if (!vmJar.exists()) {
                    throw new FileNotFoundException(allocatorConfig.getVmJarFile());
                }
                final URLClassLoader classLoader = new URLClassLoader(new URL[] {vmJar.toURI().toURL()},
                    ClassLoader.getSystemClassLoader());
                vmClass = Class.forName(allocatorConfig.getVmClassName(), true, classLoader);
            } else {
                vmClass = Class.forName(allocatorConfig.getVmClassName());
            }

            vmMain = vmClass.getDeclaredMethod("execute", String[].class);
        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException | FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AllocateResult allocate(Vm vm) {
        allocateWithSingleWorkload(vm.vmId(), vm.poolLabel(), vm.allocateState().vmOtt(), vm.workloads().get(0));
        return AllocateResult.SUCCESS;
    }

    private void allocateWithSingleWorkload(String vmId, String poolLabel, String vmOtt, Workload workload) {
        LOG.info("Allocating vm with id: " + vmId);

        var env = workload.env();

        var startupArgs = new ArrayList<>(workload.args());
        if (poolLabel.contentEquals(PORTAL_POOL_LABEL)) {
            startupArgs.addAll(List.of(
                "-portal.vm-id=" + vmId,
                "-portal.allocator-address=" + cfg.getAddress(),
                "-portal.allocator-heartbeat-period=" + cfg.getHeartbeatTimeout().dividedBy(2).toString(),
                "-portal.host=localhost",
                "-portal.allocator-token=" + vmOtt));
            if (env.containsKey("LZY_PORTAL_PKEY")) {
                startupArgs.add("-portal.iam-private-key=" + env.get("LZY_PORTAL_PKEY"));
            }
        } else {
            startupArgs.addAll(List.of(
                "--vm-id", vmId,
                "--allocator-address", cfg.getAddress(),
                "--allocator-heartbeat-period", cfg.getHeartbeatTimeout().dividedBy(2).toString(),
                "--host", "localhost"
            ));
            startupArgs.add("--allocator-token");
            startupArgs.add('"' + vmOtt + '"');
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
                    LOG.error("Error while invocation of worker/portal method 'execute': " +
                        e.getTargetException().getMessage(), e.getTargetException());
                } catch (IllegalAccessException e) {
                    LOG.error("Error while invocation of worker/portal method 'execute': " + e.getMessage(), e);
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

        var thread = vmThreads.remove(vmId);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(100);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted", e);
            } finally {
                try {
                    thread.stop();
                } catch (ThreadDeath e) {
                    // ignored
                }
            }
        }
    }

    @Override
    public List<VmEndpoint> getVmEndpoints(String vmId, @Nullable TransactionHandle transaction) {
        return List.of(
            new VmEndpoint(VmEndpointType.HOST_NAME, "localhost"),
            new VmEndpoint(VmEndpointType.INTERNAL_IP, "127.0.0.1"));
    }
}
