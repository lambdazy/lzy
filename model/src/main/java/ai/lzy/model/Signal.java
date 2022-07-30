package ai.lzy.model;

public enum Signal {
    TOUCH(0),
    HUB(1),
    KILL(9),
    TERM(10),
    CHLD(20);

    final int sig;

    Signal(int sig) {
        this.sig = sig;
    }

    public static Signal valueOf(int sigValue) {
        for (Signal value : Signal.values()) {
            if (value.sig() == sigValue) {
                return value;
            }
        }
        return TOUCH;
    }

    public int sig() {
        return sig;
    }
}
