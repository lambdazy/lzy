package ai.lzy.allocator.test.http;

import jakarta.annotation.Nonnull;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.mockwebserver.MockResponse;

public final class MockResponses {

    public static final int HTTP_SWITCH_PROTOCOLS_CODE = 101;

    private MockResponses() {
    }

    public static MockResponse websocketUpgradeSendAndClose(String message) {
        return new MockResponse().setHeader("Upgrade", "websocket")
            .setHeader("Connection", "Upgrade")
            .setResponseCode(HTTP_SWITCH_PROTOCOLS_CODE)
            .withWebSocketUpgrade(new WebSocketListener() {
                @Override
                public void onOpen(@Nonnull WebSocket webSocket, @Nonnull Response response) {
                    webSocket.send(message);
                    webSocket.close(1000, "bye");
                }
            });
    }
}
