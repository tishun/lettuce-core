package io.lettuce.core.proactive;

import io.lettuce.core.api.push.PushListener;
import io.lettuce.core.api.push.PushMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandHandler;
import io.lettuce.core.protocol.ConnectionWatchdog;
import io.lettuce.core.protocol.DefaultEndpoint;
import io.lettuce.core.pubsub.PubSubOutput;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.VoidChannelPromise;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static io.lettuce.core.protocol.CommandType.PING;
import static io.lettuce.core.protocol.CommandType.SUBSCRIBE;

/**
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Will Glozer
 */
public class ProactiveWatchdogCommandHandler<K, V> extends ChannelInboundHandlerAdapter implements PushListener {

    private static final Logger logger = Logger.getLogger(ProactiveWatchdogCommandHandler.class.getName());

    private static final String REBIND_CHANNEL = "__rebind";

    private ChannelHandlerContext context;

    private ConnectionWatchdog watchdog;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Channel {} active");

        ChannelPipeline pipeline = ctx.channel().pipeline();
        CommandHandler command = pipeline.get(CommandHandler.class);
        watchdog = pipeline.get(ConnectionWatchdog.class);
        context = ctx;

//        Command<String, String, String> rebind =
//                new Command<>(SUBSCRIBE,
//                        new PubSubOutput<>(StringCodec.UTF8),
//                        new PubSubCommandArgs<>(StringCodec.UTF8).addKey(REBIND_CHANNEL));
//
//        if (command != null) {
//            ctx.write(rebind);
//        }

        super.channelActive(ctx);
    }

    @Override
    public void onPushMessage(PushMessage message) {
        logger.info("Channel received message");

        List<String> content = message.getContent()
                .stream()
                .map( ez -> StringCodec.UTF8.decodeKey( (ByteBuffer) ez))
                .collect(Collectors.toList());

        if (content.stream().anyMatch(c -> c.contains("type=rebind"))) {
            logger.info("Attempt to rebind to new endpoint '" + getRemoteAddress(content)+"'");
            context.channel().disconnect().addListener(future -> {
                Bootstrap bootstrap = watchdog.getBootstrap();
                bootstrap.connect(getRemoteAddress(content)).addListener(futur -> {
                    if (futur.isSuccess()) {
                        logger.info("Success?");
                    } else {
                        logger.info("Failure?");
                    }
                });

            });

//            context.fireChannelInactive();
//            context.channel().connect(getRemoteAddress(content));

        }
    }

    private SocketAddress getRemoteAddress(List<String> messageContents){

        final String payload = messageContents.stream()
                .filter(c -> c.contains("to_ep"))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("to_ep not found"));

        final String toEndpoint = Arrays.stream(payload.split(";"))
                .filter( c->c.contains("to_ep"))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("to_ep not found"));

        final String addressAndPort = toEndpoint.split("=")[1];
        final String address = addressAndPort.split(":")[0];
        final int port = Integer.parseInt(addressAndPort.split(":")[1]);

        return new InetSocketAddress(address, port);
    }


    /**
     *
     * Command args for Pub/Sub connections. This implementation hides the first key as PubSub keys are not keys from the key-space.
     *
     * @author Mark Paluch
     * @since 4.2
     */
    static class PubSubCommandArgs<K, V> extends CommandArgs<K, V> {

        /**
         * @param codec Codec used to encode/decode keys and values, must not be {@code null}.
         */
        public PubSubCommandArgs(RedisCodec<K, V> codec) {
            super(codec);
        }

        /**
         *
         * @return always {@code null}.
         */
        @Override
        public ByteBuffer getFirstEncodedKey() {
            return null;
        }

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Channel inactive");

        super.channelInactive(ctx);
    }
}
