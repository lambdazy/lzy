package ai.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.NotImplementedException;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;

public class Kill implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        throw new NotImplementedException("Kill is not implemented yet");
    }
}
