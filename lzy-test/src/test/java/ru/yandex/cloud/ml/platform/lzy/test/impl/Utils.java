package ru.yandex.cloud.ml.platform.lzy.test.impl;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.model.data.types.PlainTextFileSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Utils {
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

    static ProcessBuilder javaProcess(String clazz, String[] args) {
        final List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + "/bin" + "/java");
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

    public static Slot outFileSot() {
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
                return new PlainTextFileSchema();
            }
        };
    }
}
