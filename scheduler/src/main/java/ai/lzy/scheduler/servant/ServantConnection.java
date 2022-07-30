package ai.lzy.scheduler.servant;

public interface ServantConnection {
    ServantApi api();
    void close();
}
