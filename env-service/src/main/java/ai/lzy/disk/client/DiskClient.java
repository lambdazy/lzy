package ai.lzy.disk.client;

import ai.lzy.priv.v1.LED;
import javax.annotation.Nullable;

public interface DiskClient {

    LED.Disk createDisk(@Nullable String label);

    LED.Disk getDisk(String diskId);

    LED.Disk deleteDisk(String diskId);

}
