package ai.lzy.allocator.alloc;

import ai.lzy.metrics.MetricReporter;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

import javax.inject.Singleton;

@Singleton
@Requires(bean = MetricReporter.class)
public class AllocatorMetrics {

    private static final String ALLOCATOR = "allocator";
    private static final String ALLOCATOR_PRIVATE = "allocator_private";
    private static final String VM_POOL_LABEL = "pool";

    public final Gauge activeSessions = Gauge
        .build("active_sessions", "Active Allocator Sessions")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter createSessionError = Counter
        .build("create_session_error", "Create Session Error")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter deleteSessionError = Counter
        .build("delete_session_error", "Delete Session Error")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter allocateVmFromCache = Counter
        .build("allocate_vm_from_cache", "Allocate VM from cache")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter allocateVmNew = Counter
        .build("allocate_vm_new", "Allocate new VM")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter allocationError = Counter
        .build("allocate_error", "Allocation errors")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter allocationTimeout = Counter
        .build("allocate_timeout", "Allocation timeout errors")
        .subsystem(ALLOCATOR)
        .register();

    public final Histogram allocateNewDuration = Histogram
        .build("allocate_new_time", "Allocate duration (sec)")
        .subsystem(ALLOCATOR)
        .buckets(1.0, 1.5, 2.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0)
        .register();

    public final Histogram allocateFromCacheDuration = Histogram
        .build("allocate_from_cache_time", "Allocate duration (sec)")
        .subsystem(ALLOCATOR)
        .buckets(0.5, 1.0, 1.5, 2.0, 5.0, 10.0)
        .register();

    public final Counter removeNodeError = Counter
        .build("remove_node_error", "Remove node errors")
        .subsystem(ALLOCATOR)
        .register();



    // allocator private API

    public final Counter registerSuccess = Counter
        .build("register_success", "Successfully registered VMs")
        .subsystem(ALLOCATOR_PRIVATE)
        .register();

    public final Counter registerFail = Counter
        .build("register_fail", "Failed registrations")
        .subsystem(ALLOCATOR_PRIVATE)
        .register();

    public final Counter hbUnknownVm = Counter
        .build("hb_unknown_vm", "Heartbeats from unknown VMs")
        .subsystem(ALLOCATOR_PRIVATE)
        .register();

    public final Counter hbInvalidVm = Counter
        .build("hb_invalid_vm", "Heartbeats from VMs in invalid states")
        .subsystem(ALLOCATOR_PRIVATE)
        .register();

    public final Counter hbFail = Counter
        .build("hb_fail", "Heartbeat failures")
        .subsystem(ALLOCATOR_PRIVATE)
        .register();


    // summary

    public final Gauge runningVms = Gauge
        .build("running_vms", "Running VMs")
        .subsystem(ALLOCATOR)
        .labelNames(VM_POOL_LABEL)
        .register();

    public final Gauge runningAllocations = Gauge
        .build("running_allocations", "Running Allocations")
        .subsystem(ALLOCATOR)
        .labelNames(VM_POOL_LABEL)
        .register();

    public final Gauge cachedVms = Gauge
        .build("cached_vms", "Cached VMs")
        .subsystem(ALLOCATOR)
        .labelNames(VM_POOL_LABEL)
        .register();

    public final Counter cachedVmsTime = Counter
        .build("cached_vms_time", "Cached VMs Time")
        .subsystem(ALLOCATOR)
        .labelNames(VM_POOL_LABEL)
        .register();

}
