package ai.lzy.allocator.test.http;

import jakarta.annotation.Nonnull;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;

public class MockHttpDispatcher extends Dispatcher {

    private final ConcurrentLinkedQueue<RequestHandlerEntry> handlers = new ConcurrentLinkedQueue<>();

    @Nonnull
    @Override
    public MockResponse dispatch(@Nonnull RecordedRequest recordedRequest) throws InterruptedException {
        var iterator = handlers.iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.isExhausted()) {
                iterator.remove();
                continue;
            }
            if (entry.matcher().test(recordedRequest)) {
                entry.increment();
                return entry.handler().handle(recordedRequest);
            }
        }
        return new MockResponse().setResponseCode(404);
    }

    public void addHandlerOneTime(Predicate<RecordedRequest> matcher, RequestHandler handler) {
        handlers.add(new RequestHandlerEntry(matcher, handler, 1, false));
    }

    public void addHandlerUnlimited(Predicate<RecordedRequest> matcher, RequestHandler handler) {
        handlers.add(new RequestHandlerEntry(matcher, handler, 0, true));
    }

    @Override
    public void shutdown() {
        handlers.clear();
    }

    private static final class RequestHandlerEntry {
        private final Predicate<RecordedRequest> matcher;
        private final RequestHandler handler;
        private final int limit;
        private final boolean unlimited;

        private int count = 0;

        private RequestHandlerEntry(
            Predicate<RecordedRequest> matcher,
            RequestHandler handler,
            int limit,
            boolean unlimited
        )
        {
            this.matcher = matcher;
            this.handler = handler;
            this.limit = limit;
            this.unlimited = unlimited;
        }

        public Predicate<RecordedRequest> matcher() {
            return matcher;
        }

        public RequestHandler handler() {
            return handler;
        }

        public void increment() {
            count++;
        }

        public boolean isExhausted() {
            return !unlimited && count >= limit;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (RequestHandlerEntry) obj;
            return Objects.equals(this.matcher, that.matcher) &&
                Objects.equals(this.handler, that.handler) &&
                this.limit == that.limit &&
                this.unlimited == that.unlimited;
        }

        @Override
        public int hashCode() {
            return Objects.hash(matcher, handler, limit, unlimited);
        }

        @Override
        public String toString() {
            return "RequestHandlerEntry[" +
                "matcher=" + matcher + ", " +
                "handler=" + handler + ", " +
                "limit=" + limit + ", " +
                "unlimited=" + unlimited + ']';
        }
    }
}
