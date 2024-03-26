package io.lettuce.core.rebind;

import io.lettuce.core.api.push.PushListener;
import io.lettuce.core.api.push.PushMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandHandler;
import io.lettuce.core.pubsub.PubSubCommandHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Will Glozer
 */
@ChannelHandler.Sharable
public class RebindHandler<K, V> extends ChannelInboundHandlerAdapter implements PushListener {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(RebindHandler.class);

    private final boolean debugEnabled = logger.isDebugEnabled();

    private ChannelHandlerContext context;


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ChannelPipeline pipeline = ctx.channel().pipeline();
        CommandHandler command = pipeline.get(CommandHandler.class);
        context = ctx;

        PubSubCommandHandler<?, ?> commandHandler = pipeline.get(PubSubCommandHandler.class);
        commandHandler.getEndpoint().addListener(this);

        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof RebindCompleteEvent) {
            if (debugEnabled) {
                logger.debug("Rebind completed at {}", LocalTime.now().toString());
            }

            // disconnect the current channel and fire a re-bind event with the new address
            context.channel().close().awaitUninterruptibly();
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void onPushMessage(PushMessage message) {
        if (!message.getType().equals("message")) {
            return;
        }

        List<String> content = message.getContent().stream()
                .map(ez -> ez instanceof ByteBuffer ? StringCodec.UTF8.decodeKey((ByteBuffer) ez) : ez.toString())
                .collect(Collectors.toList());

        if (content.stream().anyMatch(c -> c.contains("type=rebind"))) {
            if (debugEnabled) {
                logger.debug("Attempt to rebind to new endpoint '{}'", getRemoteAddress(content));
            }
            context.fireUserEventTriggered(new RebindInitiatedEvent(getRemoteAddress(content)));
        }
    }

    private SocketAddress getRemoteAddress(List<String> messageContents) {

        final String payload = messageContents.stream().filter(c -> c.contains("to_ep")).findFirst()
                .orElse("type=rebind;from_ep=localhost:6479;to_ep=localhost:6379;until_s=10");

        final String toEndpoint = Arrays.stream(payload.split(";")).filter(c -> c.contains("to_ep")).findFirst()
                .orElse("to_ep=localhost:6479");

        final String addressAndPort = toEndpoint.split("=")[1];
        final String address = addressAndPort.split(":")[0];
        final int port = Integer.parseInt(addressAndPort.split(":")[1]);

        return new InetSocketAddress(address, port);
    }

}
