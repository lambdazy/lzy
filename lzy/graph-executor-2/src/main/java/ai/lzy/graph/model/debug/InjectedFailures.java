package ai.lzy.graph.model.debug;

public class InjectedFailures {
    public static final class TerminateException extends Error {
        public TerminateException() {
        }

        public TerminateException(String message) {
            super(message);
        }
    }
}
