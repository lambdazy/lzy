package ai.lzy.allocator.test.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;

public interface RequestHandler {
    MockResponse handle(RecordedRequest request);
}
