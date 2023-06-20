package ai.lzy.fs.commands;

import jakarta.annotation.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public interface LzyCommand {
    int execute(CommandLine command) throws Exception;

    @Nullable
    default CommandLine parse(CommandLine command, Options options) {
        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, command.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp(this.getClass().getName(), options);
            return null;
        }
        return localCmd;
    }
}
