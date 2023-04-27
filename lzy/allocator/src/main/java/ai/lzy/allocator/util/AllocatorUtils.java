package ai.lzy.allocator.util;

import ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator;
import ai.lzy.allocator.model.Vm;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public final class AllocatorUtils {
    private AllocatorUtils() { }

    @Nullable
    public static String getClusterId(Vm vm) {
        var meta = vm.allocateState().allocatorMeta();
        if (meta == null) {
            return null;
        }

        return meta.get(KuberVmAllocator.CLUSTER_ID_KEY);
    }

    public static void readToLog(Logger log, String logPrefix, InputStream is) {
        try (var reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info("{}: {}", logPrefix, line);
            }
        } catch (IOException e) {
            log.warn("Failed to read from stream", e);
        }
    }

}
