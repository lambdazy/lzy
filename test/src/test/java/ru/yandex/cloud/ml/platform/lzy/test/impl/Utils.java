package ru.yandex.cloud.ml.platform.lzy.test.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;

public class Utils {
    public static class Defaults {
        public static final int    TIMEOUT_SEC     = 30;
        public static final int    SERVANT_PORT    = 9999;
        public static final int    S3_PORT         = 8001;
        public static final int    WHITEBOARD_PORT = 8999;
        public static final int    SERVANT_FS_PORT = 9998;
        public static final int    DEBUG_PORT      = 5006;
        public static final String LZY_MOUNT       = "/tmp/lzy";
    }

    public static boolean waitFlagUp(Supplier<Boolean> supplier, long timeout, TimeUnit unit) {
        boolean flag = false;
        final long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TimeUnit.MILLISECONDS.convert(timeout, unit)) {
            flag = supplier.get();
            if (flag) {
                break;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return flag;
    }

    static ProcessBuilder javaProcess(String clazz, String[] args, String[] systemPropertiesArgs) {
        final List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin" + "/java");
        command.addAll(Arrays.asList(systemPropertiesArgs));
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(clazz);
        command.addAll(Arrays.asList(args));
        return new ProcessBuilder(command);
    }

    static String bashEscape(String command) {
        command = command.replace("\\", "\\\\");
        command = command.replace("\"", "\\\"");
        command = command.replace("$", "\\$");
        command = command.replace("&", "\\&");
        command = command.replace("<", "\\<");
        command = command.replace(">", "\\>");
        command = command.replace(" ", "\\ ");
        return command;
    }

    public static Slot outFileSlot() {
        return new Slot() {
            @Override
            public String name() {
                return "";
            }

            @Override
            public Media media() {
                return Media.FILE;
            }

            @Override
            public Direction direction() {
                return Direction.OUTPUT;
            }

            @Override
            public DataSchema contentType() {
                return DataSchema.plain;
            }
        };
    }
    public static Slot inFileSlot() {
        return new Slot() {
            @Override
            public String name() {
                return "";
            }

            @Override
            public Media media() {
                return Media.FILE;
            }

            @Override
            public Direction direction() {
                return Direction.INPUT;
            }

            @Override
            public DataSchema contentType() {
                return DataSchema.plain;
            }
        };
    }

    public static String lastLine(String s) {
        final String[] split = s.split("\n");
        return split[split.length - 1];
    }

}
