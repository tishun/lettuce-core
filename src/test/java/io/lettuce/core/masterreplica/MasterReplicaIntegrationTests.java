package io.lettuce.core.masterreplica;

import static io.lettuce.TestTags.INTEGRATION_TEST;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.lettuce.core.AbstractRedisClientTest;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.role.RedisInstance;
import io.lettuce.core.models.role.RedisNodeDescription;
import io.lettuce.core.models.role.RoleParser;
import io.lettuce.test.WithPassword;
import io.lettuce.test.condition.EnabledOnCommand;
import io.lettuce.test.settings.TestSettings;

/**
 * Integration tests for master/replica via {@link MasterReplica}.
 *
 * @author Mark Paluch
 */
@Tag(INTEGRATION_TEST)
class MasterReplicaIntegrationTests extends AbstractRedisClientTest {

    private RedisURI masterURI = RedisURI.Builder.redis(host, TestSettings.port(3)).withPassword(passwd)
            .withClientName("my-client").withDatabase(5).build();

    private StatefulRedisMasterReplicaConnection<String, String> connection;

    private RedisURI upstream;

    private RedisURI replica;

    private RedisCommands<String, String> connection1;

    private RedisCommands<String, String> connection2;

    @BeforeEach
    void before() {

        RedisURI node1 = RedisURI.Builder.redis(host, TestSettings.port(3)).withDatabase(2).build();
        RedisURI node2 = RedisURI.Builder.redis(host, TestSettings.port(4)).withDatabase(2).build();

        connection1 = client.connect(node1).sync();
        connection2 = client.connect(node2).sync();

        RedisInstance node1Instance = RoleParser.parse(this.connection1.role());
        RedisInstance node2Instance = RoleParser.parse(this.connection2.role());

        if (node1Instance.getRole().isUpstream() && node2Instance.getRole().isReplica()) {
            upstream = node1;
            replica = node2;
        } else if (node2Instance.getRole().isUpstream() && node1Instance.getRole().isReplica()) {
            upstream = node2;
            replica = node1;
        } else {
            assumeTrue(false,
                    String.format("Cannot run the test because I don't have a distinct master and replica but %s and %s",
                            node1Instance, node2Instance));
        }

        WithPassword.enableAuthentication(this.connection1);
        this.connection1.auth(passwd);
        this.connection1.configSet("masterauth", passwd.toString());

        WithPassword.enableAuthentication(this.connection2);
        this.connection2.auth(passwd);
        this.connection2.configSet("masterauth", passwd.toString());

        connection = MasterReplica.connect(client, StringCodec.UTF8, masterURI);
        connection.setReadFrom(ReadFrom.REPLICA);
    }

    @AfterEach
    void after() {

        if (connection1 != null) {
            WithPassword.disableAuthentication(connection1);
            connection1.configRewrite();
            connection1.getStatefulConnection().close();
        }

        if (connection2 != null) {
            WithPassword.disableAuthentication(connection2);
            connection2.configRewrite();
            connection2.getStatefulConnection().close();
        }

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testMasterReplicaReadFromMaster() {

        connection.setReadFrom(ReadFrom.UPSTREAM);
        String server = connection.sync().info("server");

        Pattern pattern = Pattern.compile("tcp_port:(\\d+)");
        Matcher matcher = pattern.matcher(server);

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("" + upstream.getPort());
    }

    @Test
    void testMasterReplicaReadFromReplica() {

        String server = connection.sync().info("server");

        Pattern pattern = Pattern.compile("tcp_port:(\\d+)");
        Matcher matcher = pattern.matcher(server);

        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("" + replica.getPort());
        assertThat(connection.getReadFrom()).isEqualTo(ReadFrom.REPLICA);
    }

    @Test
    void testMasterReplicaReadWrite() {

        RedisCommands<String, String> redisCommands = connection.sync();
        redisCommands.set(key, value);
        redisCommands.waitForReplication(1, 100);

        assertThat(redisCommands.get(key)).isEqualTo(value);
    }

    @Test
    void testConnectToReplica() {

        connection.close();

        RedisURI replicaUri = RedisURI.Builder.redis(host, TestSettings.port(4)).withPassword(passwd).build();
        connection = MasterReplica.connect(client, StringCodec.UTF8, replicaUri);

        RedisCommands<String, String> sync = connection.sync();
        sync.set(key, value);
    }

    @Test
    void noReplicaForRead() {

        connection.setReadFrom(new ReadFrom() {

            @Override
            public List<RedisNodeDescription> select(Nodes nodes) {
                return Collections.emptyList();
            }

        });

        assertThatThrownBy(() -> replicaCall(connection)).isInstanceOf(RedisException.class);
    }

    @Test
    void masterReplicaConnectionShouldSetClientName() {

        assertThat(connection.sync().clientGetname()).isEqualTo(masterURI.getClientName());
        connection.sync().quit();
        assertThat(connection.sync().clientGetname()).isEqualTo(masterURI.getClientName());

        connection.close();
    }

    @Test
    @EnabledOnCommand("ACL")
    void testConnectToReplicaWithAcl() {

        connection.close();

        RedisURI replicaUri = RedisURI.Builder.redis(host, TestSettings.port(900 + 6)).withAuthentication("default", passwd)
                .build();
        connection = MasterReplica.connect(client, StringCodec.UTF8, replicaUri);

        RedisCommands<String, String> sync = connection.sync();

        assertThat(sync.ping()).isEqualTo("PONG");
    }

    static String replicaCall(StatefulRedisMasterReplicaConnection<String, String> connection) {
        return connection.sync().info("replication");
    }

}
