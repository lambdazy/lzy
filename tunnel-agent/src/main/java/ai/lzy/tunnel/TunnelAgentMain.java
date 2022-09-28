package ai.lzy.tunnel;

import ai.lzy.util.grpc.ChannelBuilder;
import com.google.common.net.HostAndPort;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class TunnelAgentMain {

    public static void main(String[] args) {;
        ServerBuilder<?> builder = NettyServerBuilder
            .forPort(1234)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

        // TODO
    }
}
