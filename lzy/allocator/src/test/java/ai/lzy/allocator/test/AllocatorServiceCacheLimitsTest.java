package ai.lzy.allocator.test;

import ai.lzy.allocator.configs.ServiceConfig;
import com.google.protobuf.util.Durations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class AllocatorServiceCacheLimitsTest extends AllocatorApiTestBase {
    private ServiceConfig.CacheLimits cacheLimits;

    @Before
    public void before() throws IOException {
        cacheLimits = allocatorContext.getBean(ServiceConfig.CacheLimits.class);
    }

    @After
    public void after() {
        super.tearDown();
    }

    @Test
    public void noLimits() throws Exception {
        var sid = createSession(Durations.fromSeconds(10));

        cacheLimits.setUserLimit(Integer.MAX_VALUE);
        cacheLimits.setSessionLimit(Integer.MAX_VALUE);
        cacheLimits.setSessionPoolLimit(null);
        cacheLimits.setAnySessionPoolLimit(Integer.MAX_VALUE);

        var vm1 = allocateVm(sid, "S", null);
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());

        var vm2 = allocateVm(sid, "S", null);
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());

        var vm3 = allocateVm(sid, "S", null);
        Assert.assertEquals(3, (int) metrics.runningVms.labels("S").get());

        freeVm(vm1.vmId());
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());

        freeVm(vm2.vmId());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(2, (int) metrics.cachedVms.labels("S").get());

        freeVm(vm3.vmId());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(3, (int) metrics.cachedVms.labels("S").get());
    }

    @Test
    public void userLimitSingleSession() throws Exception {
        var sid = createSession(Durations.fromSeconds(10));

        cacheLimits.setUserLimit(1);
        cacheLimits.setSessionLimit(Integer.MAX_VALUE);
        cacheLimits.setSessionPoolLimit(null);

        var vm1 = allocateVm(sid, "S", null);
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());

        var vm2 = allocateVm(sid, "S", null);
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());

        freeVm(vm1.vmId());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());

        freeVm(vm2.vmId());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());
    }

    @Test
    public void userLimitMultipleSessions() throws Exception {
        var sid1 = createSession("user1", Durations.fromSeconds(10));
        var sid2 = createSession("user1", Durations.fromSeconds(10));

        cacheLimits.setUserLimit(2);
        cacheLimits.setSessionLimit(1);
        cacheLimits.setSessionPoolLimit(null);

        var vm1 = allocateVm(sid1, "S", null);
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());

        var vm2 = allocateVm(sid2, "S", null);
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());

        var vm3 = allocateVm(sid1, "S", null);
        Assert.assertEquals(3, (int) metrics.runningVms.labels("S").get());

        freeVm(vm1.vmId());
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());

        freeVm(vm2.vmId());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(2, (int) metrics.cachedVms.labels("S").get());

        freeVm(vm3.vmId());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(2, (int) metrics.cachedVms.labels("S").get());
    }

    @Test
    public void poolLimit() throws Exception {
        var sid = createSession(Durations.fromSeconds(10));

        cacheLimits.setUserLimit(5);
        cacheLimits.setSessionLimit(3);
        cacheLimits.setSessionPoolLimit(Map.of("S", 1, "M", 1));

        var vm1 = allocateVm(sid, "S", null);
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("M").get());

        var vm2 = allocateVm(sid, "S", null);
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("M").get());

        var vm3 = allocateVm(sid, "M", null);
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("M").get());

        var vm4 = allocateVm(sid, "M", null);
        Assert.assertEquals(2, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(2, (int) metrics.runningVms.labels("M").get());

        freeVm(vm1.vmId());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(2, (int) metrics.runningVms.labels("M").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("M").get());

        freeVm(vm2.vmId());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(2, (int) metrics.runningVms.labels("M").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.cachedVms.labels("M").get());

        freeVm(vm3.vmId());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.runningVms.labels("M").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("M").get());

        freeVm(vm4.vmId());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("S").get());
        Assert.assertEquals(0, (int) metrics.runningVms.labels("M").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("S").get());
        Assert.assertEquals(1, (int) metrics.cachedVms.labels("M").get());
    }
}
