package ai.lzy.service.operations;

import ai.lzy.util.grpc.ProtoPrinter;
import org.apache.logging.log4j.Logger;

public interface LogContextAwareStep {
    ProtoPrinter.Printer safePrinter();

    ProtoPrinter.Printer printer();

    Logger log();

    String logPrefix();
}
