package ai.lzy.scheduler.exp;

import jakarta.inject.Singleton;

@Singleton
public class ThreadServantAllocatorExp extends ServantAllocatorExpBase {

    @Override
    protected void requestAllocation(ServantSpec servantSpec) throws ServantAllocationException {

    }

    @Override
    protected void cancelAllocation(ServantSpec servantSpec) {

    }

    @Override
    protected void deallocate(String servantId) {

    }
}
