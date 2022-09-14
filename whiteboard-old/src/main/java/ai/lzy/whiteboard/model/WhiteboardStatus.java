package ai.lzy.whiteboard.model;

public interface WhiteboardStatus {
    Whiteboard whiteboard();

    State state();

    enum State {
        CREATED,
        COMPLETED,
        ERRORED
    }

    class Impl implements WhiteboardStatus {
        private final Whiteboard whiteboard;
        private final State state;

        public Impl(Whiteboard whiteboard, State state) {
            this.whiteboard = whiteboard;
            this.state = state;
        }

        @Override
        public Whiteboard whiteboard() {
            return whiteboard;
        }

        @Override
        public State state() {
            return state;
        }

        public String toString() {
            return "whiteboard: {" + whiteboard + "}, state: " + state + "}";
        }
    }
}
