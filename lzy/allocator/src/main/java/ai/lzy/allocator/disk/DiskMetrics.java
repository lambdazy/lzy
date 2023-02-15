package ai.lzy.allocator.disk;

import ai.lzy.metrics.MetricReporter;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import jakarta.inject.Named;

import javax.inject.Singleton;

@Singleton
public class DiskMetrics {

    private static final String ALLOCATOR = "allocator";

    public DiskMetrics(@Named("AllocatorMetricReporter") MetricReporter ignored) {
    }

    // CREATE DISK

    public final Counter createDiskExisting = Counter
        .build("create_disk_existing", "Create disk from existing cloud disk")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter createDiskNewStart = Counter
        .build("create_disk_new_start", "Create new disk (started requests)")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter createDiskNewFinish = Counter
        .build("create_disk_new_finish", "Create new disk (finished requests)")
        .subsystem(ALLOCATOR)
        .register();

    public final Histogram createNewDiskDuration = Histogram
        .build("create_new_disk_duration", "Create new disk duration (sec)")
        .subsystem(ALLOCATOR)
        .buckets(1.0, 2.0, 5.0, 10.0, 15.0)
        .register();

    public final Counter createDiskError = Counter
        .build("create_disk_error", "Disk creation errors")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter createDiskRetryableError = Counter
        .build("create_disk_retryable_error", "Disk creation errors (retryable)")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter createDiskTimeout = Counter
        .build("create_disk_timeout", "Disk creation timeout")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter createDiskAlreadyExists = Counter
        .build("create_disk_already_exists", "Disk already exists")
        .subsystem(ALLOCATOR)
        .register();


    // CLONE DISK

    public final Counter cloneDiskStart = Counter
        .build("clone_disk_start", "Clone disk (started requests)")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter cloneDiskFinish = Counter
        .build("clone_disk_finish", "Clone disk (finished requests)")
        .subsystem(ALLOCATOR)
        .register();

    public final Histogram cloneDiskDuration = Histogram
        .build("clone_disk_duration", "Clone disk duration (sec)")
        .subsystem(ALLOCATOR)
        .buckets(1.0, 2.0, 5.0, 10.0)
        .register();

    public final Counter cloneDiskError = Counter
        .build("clone_disk_error", "Disk clone errors")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter cloneDiskRetryableError = Counter
        .build("clone_disk_retryable_error", "Disk clone errors (retryable)")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter cloneDiskTimeout = Counter
        .build("clone_disk_timeout", "Disk clone timeout")
        .subsystem(ALLOCATOR)
        .register();


    // DELETE DISK

    public final Counter deleteDiskStart = Counter
        .build("delete_disk_start", "Delete disk (started requests)")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter deleteDiskFinish = Counter
        .build("delete_disk_finish", "Delete disk (finished requests)")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter deleteDiskError = Counter
        .build("delete_disk_error", "Disk deletion errors")
        .subsystem(ALLOCATOR)
        .register();

    public final Counter deleteDiskRetryableError = Counter
        .build("delete_disk_retryable_error", "Disk deletion errors (retryable)")
        .subsystem(ALLOCATOR)
        .register();

    public final Histogram deleteDiskDuration = Histogram
        .build("delete_disk_duration", "Delete disk duration (sec)")
        .subsystem(ALLOCATOR)
        .buckets(1.0, 2.0, 5.0, 10.0)
        .register();
}
