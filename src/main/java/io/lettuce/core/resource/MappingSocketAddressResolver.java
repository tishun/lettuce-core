package io.lettuce.core.resource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.function.Function;

import io.lettuce.core.RedisURI;
import io.lettuce.core.internal.HostAndPort;
import io.lettuce.core.internal.LettuceAssert;

/**
 * Mapping {@link SocketAddressResolver} that allows mapping of {@link io.lettuce.core.RedisURI} host and port components to
 * redirect connection endpoint coordinates using a {@link Function mapping function}.
 *
 * @author Mark Paluch
 * @since 5.1
 */
public class MappingSocketAddressResolver extends SocketAddressResolver {

    private final Function<HostAndPort, HostAndPort> mappingFunction;

    private final DnsResolver dnsResolver;

    /**
     * Create a new {@link SocketAddressResolver} given {@link Function mapping function}.
     *
     * @param mappingFunction must not be {@code null}.
     * @since 6.1
     */
    private MappingSocketAddressResolver(Function<HostAndPort, HostAndPort> mappingFunction) {
        this(DnsResolver.unresolved(), mappingFunction);
    }

    /**
     * Create a new {@link SocketAddressResolver} given {@link DnsResolver} and {@link Function mapping function}.
     *
     * @param dnsResolver must not be {@code null}.
     * @param mappingFunction must not be {@code null}.
     */
    private MappingSocketAddressResolver(DnsResolver dnsResolver, Function<HostAndPort, HostAndPort> mappingFunction) {

        super(dnsResolver);

        LettuceAssert.notNull(mappingFunction, "Mapping function must not be null!");
        this.dnsResolver = dnsResolver;
        this.mappingFunction = mappingFunction;
    }

    /**
     * Create a new {@link SocketAddressResolver} given {@link DnsResolver} and {@link Function mapping function}.
     *
     * @param mappingFunction must not be {@code null}.
     * @return the {@link MappingSocketAddressResolver}.
     * @since 6.1
     */
    public static MappingSocketAddressResolver create(Function<HostAndPort, HostAndPort> mappingFunction) {
        return new MappingSocketAddressResolver(mappingFunction);
    }

    /**
     * Create a new {@link SocketAddressResolver} given {@link DnsResolver} and {@link Function mapping function}.
     *
     * @param dnsResolver must not be {@code null}.
     * @param mappingFunction must not be {@code null}.
     * @return the {@link MappingSocketAddressResolver}.
     */
    public static MappingSocketAddressResolver create(DnsResolver dnsResolver,
            Function<HostAndPort, HostAndPort> mappingFunction) {
        return new MappingSocketAddressResolver(dnsResolver, mappingFunction);
    }

    @Override
    public SocketAddress resolve(RedisURI redisURI) {

        if (redisURI.getSocket() != null) {
            return getDomainSocketAddress(redisURI);
        }

        HostAndPort hostAndPort = HostAndPort.of(redisURI.getHost(), redisURI.getPort());

        HostAndPort mapped = mappingFunction.apply(hostAndPort);
        if (mapped == null) {
            throw new IllegalStateException("Mapping function must not return null for HostAndPort");
        }

        try {
            return doResolve(mapped);
        } catch (UnknownHostException e) {
            return new InetSocketAddress(redisURI.getHost(), redisURI.getPort());
        }
    }

    private SocketAddress doResolve(HostAndPort mapped) throws UnknownHostException {

        InetAddress[] inetAddress = dnsResolver.resolve(mapped.getHostText());

        if (inetAddress.length == 0) {
            return InetSocketAddress.createUnresolved(mapped.getHostText(), mapped.getPort());
        }

        return new InetSocketAddress(inetAddress[0], mapped.getPort());
    }

}
