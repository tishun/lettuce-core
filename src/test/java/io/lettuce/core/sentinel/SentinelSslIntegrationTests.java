package io.lettuce.core.sentinel;

import static io.lettuce.TestTags.INTEGRATION_TEST;
import static io.lettuce.test.settings.TestSettings.sslPort;
import static io.lettuce.test.settings.TlsSettings.createAndSaveTestTruststore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.lettuce.RedisBug;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.internal.HostAndPort;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DnsResolver;
import io.lettuce.core.resource.MappingSocketAddressResolver;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import io.lettuce.test.CanConnect;
import io.lettuce.test.LettuceExtension;
import io.lettuce.test.resource.FastShutdown;
import io.lettuce.test.settings.TestSettings;

/**
 * Integration tests for Sentinel usage.
 *
 * @author Mark Paluch
 */
@Tag(INTEGRATION_TEST)
@ExtendWith(LettuceExtension.class)
class SentinelSslIntegrationTests extends TestSupport {

    private static File truststoreFile;

    private final ClientResources clientResources;

    private static Map<Integer, Integer> portMap = new HashMap<>();
    static {
        portMap.put(26379, 26822);
        portMap.put(6482, 8443);
        portMap.put(6483, 8444);
    }

    @Inject
    SentinelSslIntegrationTests(ClientResources clientResources) {

        this.clientResources = clientResources.mutate()
                .socketAddressResolver(MappingSocketAddressResolver.create(DnsResolver.jvmDefault(), hostAndPort -> {
                    int port = hostAndPort.getPort();
                    if (portMap.containsKey(port)) {
                        return HostAndPort.of(hostAndPort.getHostText(), portMap.get(port));
                    }

                    return hostAndPort;
                })).build();
    }

    @BeforeAll
    static void beforeAll() {
        assumeTrue(CanConnect.to(TestSettings.host(), sslPort()), "Assume that stunnel runs on port 6443");
        Path path2 = createAndSaveTestTruststore("redis-standalone-sentinel-controlled",
                Paths.get("redis-standalone-sentinel-controlled/work/tls"), "changeit");
        truststoreFile = path2.toFile();
        assertThat(truststoreFile).exists();
    }

    @Test
    void shouldConnectSentinelDirectly() {

        RedisURI redisURI = RedisURI.create("rediss://" + TestSettings.host() + ":" + RedisURI.DEFAULT_SENTINEL_PORT);
        redisURI.setVerifyPeer(false);

        RedisClient client = RedisClient.create(clientResources);
        StatefulRedisSentinelConnection<String, String> connection = client.connectSentinel(redisURI);

        assertThat(connection.sync().getMasterAddrByName("mymaster")).isNotNull();

        connection.close();
        FastShutdown.shutdown(client);
    }

    @Test
    void shouldConnectToMasterUsingSentinel() {

        RedisURI redisURI = RedisURI.create("rediss-sentinel://" + TestSettings.host() + ":" + RedisURI.DEFAULT_SENTINEL_PORT
                + "?sentinelMasterId=mymaster");
        SslOptions options = SslOptions.builder().truststore(truststoreFile, "changeit").build();

        RedisClient client = RedisClient.create(clientResources);
        client.setOptions(ClientOptions.builder().sslOptions(options).build());
        StatefulRedisConnection<String, String> connection = client.connect(redisURI);

        assertThat(connection.sync().ping()).isNotNull();

        connection.close();
        FastShutdown.shutdown(client);
    }

}
