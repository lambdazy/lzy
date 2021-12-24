package ru.yandex.cloud.ml.platform.lzy.test;

public interface LzyTest {
    LzyTerminalTestContext terminalContext();
    LzyServerTestContext serverContext();
    LzyKharonTestContext kharonContext();
    LzySnapshotTestContext whiteboardContext();

    String defaultLzyMount();
    int defaultServantPort();
    int defaultTimeoutSec();
    int s3Port();
}
