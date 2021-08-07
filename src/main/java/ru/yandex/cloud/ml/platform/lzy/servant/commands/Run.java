package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantCommand;

import java.util.Arrays;

public class Run implements ServantCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        System.out.println(Arrays.toString(command.getArgs()));
        return 0;
    }
}
