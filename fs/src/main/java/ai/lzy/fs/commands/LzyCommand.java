package ai.lzy.fs.commands;

import org.apache.commons.cli.CommandLine;

public interface LzyCommand {
    int execute(CommandLine command) throws Exception;
}
