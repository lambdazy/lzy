package ai.lzy.service.gc;

public interface GarbageCollectorInterceptor {
    void notifyStartDeleteAllocatorSessionOp(String opId);
}
