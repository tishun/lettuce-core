package io.lettuce.authx;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.PubSubTestListener;
import io.lettuce.test.Wait;
import io.lettuce.test.env.Endpoints;
import io.lettuce.test.env.Endpoints.Endpoint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import redis.clients.authentication.core.TokenAuthConfig;
import redis.clients.authentication.entraid.EntraIDTokenAuthConfigBuilder;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.lettuce.TestTags.ENTRA_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(ENTRA_ID)
public class EntraIdIntegrationTests {

    private static final EntraIdTestContext testCtx = EntraIdTestContext.DEFAULT;

    private static TokenBasedRedisCredentialsProvider credentialsProvider;

    private static RedisClient client;

    private static Endpoint standalone;

    @BeforeAll
    public static void setup() {
        standalone = Endpoints.DEFAULT.getEndpoint("standalone-entraid-acl");
        if (standalone != null) {
            Assumptions.assumeTrue(testCtx.getClientId() != null && testCtx.getClientSecret() != null,
                    "Skipping EntraID tests. Azure AD credentials not provided!");
            // Configure timeout options to assure fast test failover
            ClusterClientOptions clientOptions = ClusterClientOptions.builder()
                    .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(1)).build())
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(1)))
                    // enable re-authentication
                    .reauthenticateBehavior(ClientOptions.ReauthenticateBehavior.ON_NEW_CREDENTIALS).build();

            TokenAuthConfig tokenAuthConfig = EntraIDTokenAuthConfigBuilder.builder().clientId(testCtx.getClientId())
                    .secret(testCtx.getClientSecret()).authority(testCtx.getAuthority()).scopes(testCtx.getRedisScopes())
                    .expirationRefreshRatio(0.0000001F).build();

            credentialsProvider = TokenBasedRedisCredentialsProvider.create(tokenAuthConfig);

            RedisURI uri = RedisURI.create((standalone.getEndpoints().get(0)));
            uri.setCredentialsProvider(credentialsProvider);
            client = RedisClient.create(uri);
            client.setOptions(clientOptions);

        }
    }

    @AfterAll
    public static void cleanup() {
        if (credentialsProvider != null) {
            credentialsProvider.close();
        }
    }

    // T.1.1
    // Verify authentication using Azure AD with service principals using Redis Standalone client
    @Test
    public void standaloneWithSecret_azureServicePrincipalIntegrationTest() throws ExecutionException, InterruptedException {
        assumeTrue(standalone != null, "Skipping EntraID tests. Redis host with enabled EntraId not provided!");

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> sync = connection.sync();
            String key = UUID.randomUUID().toString();
            sync.set(key, "value");
            assertThat(connection.sync().get(key)).isEqualTo("value");
            assertThat(connection.async().get(key).get()).isEqualTo("value");
            assertThat(connection.reactive().get(key).block()).isEqualTo("value");
            sync.del(key);
        }
    }

    // T.2.2
    // Test that the Redis client is not blocked/interrupted during token renewal.
    @Test
    public void renewalDuringOperationsTest() throws InterruptedException {
        assumeTrue(standalone != null, "Skipping EntraID tests. Redis host with enabled EntraId not provided!");

        // Counter to track the number of command cycles
        AtomicInteger commandCycleCount = new AtomicInteger(0);

        // Start a thread to continuously send Redis commands
        Thread commandThread = new Thread(() -> {
            try (StatefulRedisConnection<String, String> connection = client.connect()) {
                RedisAsyncCommands<String, String> async = connection.async();
                for (int i = 1; i <= 10; i++) {
                    // Start a transaction with SET and INCRBY commands
                    RedisFuture<String> multi = async.multi();
                    RedisFuture<String> set = async.set("key", "1");
                    RedisFuture<Long> incrby = async.incrby("key", 1);
                    RedisFuture<TransactionResult> exec = async.exec();
                    TransactionResult results = exec.get(1, TimeUnit.SECONDS);

                    // Increment the command cycle count after each execution
                    commandCycleCount.incrementAndGet();

                    // Verify the results from EXEC
                    assertThat(results).hasSize(2); // We expect 2 responses: SET and INCRBY

                    // Check the response from each command in the transaction
                    assertThat((String) results.get(0)).isEqualTo("OK"); // SET "key" = "1"
                    assertThat((Long) results.get(1)).isEqualTo(2L); // INCRBY "key" by 1, expected result is 2
                }
            } catch (Exception e) {
                fail("Command execution failed during token refresh", e);
            }
        });

        commandThread.start();

        CountDownLatch latch = new CountDownLatch(10); // Wait for at least 10 token renewals

        credentialsProvider.credentials().subscribe(cred -> {
            latch.countDown(); // Signal each renewal as it's received
        });

        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue(); // Wait to reach 10 renewals
        commandThread.join(); // Wait for the command thread to finish

        // Verify that at least 10 command cycles were executed during the test
        assertThat(commandCycleCount.get()).isGreaterThanOrEqualTo(10);
    }

    // T.2.2
    // Test basic Pub/Sub functionality is not blocked/interrupted during token renewal.
    @Test
    public void renewalDuringPubSubOperationsTest() throws InterruptedException {
        assumeTrue(standalone != null, "Skipping EntraID tests. Redis host with enabled EntraId not provided!");

        try (StatefulRedisPubSubConnection<String, String> connectionPubSub = client.connectPubSub();
                StatefulRedisPubSubConnection<String, String> connectionPubSub1 = client.connectPubSub()) {

            PubSubTestListener listener = new PubSubTestListener();
            connectionPubSub.addListener(listener);
            connectionPubSub.sync().subscribe("channel");

            // Start a thread to continuously send Redis commands
            Thread pubsubThread = new Thread(() -> {
                for (int i = 1; i <= 100; i++) {
                    connectionPubSub1.sync().publish("channel", "message");
                }
            });

            pubsubThread.start();

            CountDownLatch latch = new CountDownLatch(10);
            credentialsProvider.credentials().subscribe(cred -> {
                latch.countDown();
            });

            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue(); // Wait for at least 10 token renewals
            pubsubThread.join(); // Wait for the pub/sub thread to finish

            // Verify that all messages were received
            Wait.untilEquals(100, () -> listener.getMessages().size()).waitOrTimeout();
            assertThat(listener.getMessages()).allMatch(msg -> msg.equals("message"));
        }
    }

}
