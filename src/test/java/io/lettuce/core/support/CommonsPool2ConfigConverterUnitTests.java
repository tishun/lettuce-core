package io.lettuce.core.support;

import static io.lettuce.TestTags.UNIT_TEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CommonsPool2ConfigConverter}.
 *
 * @author Mark Paluch
 */
@Tag(UNIT_TEST)
class CommonsPool2ConfigConverterUnitTests {

    @Test
    void shouldAdaptConfiguration() {

        GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMinIdle(2);
        config.setMaxIdle(12);
        config.setMaxTotal(13);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestOnCreate(true);

        BoundedPoolConfig result = CommonsPool2ConfigConverter.bounded(config);

        assertThat(result.getMinIdle()).isEqualTo(2);
        assertThat(result.getMaxIdle()).isEqualTo(12);
        assertThat(result.getMaxTotal()).isEqualTo(13);
        assertThat(result.isTestOnAcquire()).isTrue();
        assertThat(result.isTestOnCreate()).isTrue();
        assertThat(result.isTestOnRelease()).isTrue();
    }

    @Test
    void shouldConvertNegativeValuesToMaxSize() {

        GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMaxIdle(-1);
        config.setMaxTotal(-1);

        BoundedPoolConfig result = CommonsPool2ConfigConverter.bounded(config);

        assertThat(result.getMaxIdle()).isEqualTo(Integer.MAX_VALUE);
        assertThat(result.getMaxTotal()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void shouldAdaptTestOnAcquire() {

        booleanTester(true, BaseObjectPoolConfig::setTestOnBorrow, BasePoolConfig::isTestOnAcquire);
        booleanTester(false, BaseObjectPoolConfig::setTestOnBorrow, BasePoolConfig::isTestOnAcquire);
    }

    @Test
    void shouldAdaptTestOnCreate() {

        booleanTester(true, BaseObjectPoolConfig::setTestOnCreate, BasePoolConfig::isTestOnCreate);
        booleanTester(false, BaseObjectPoolConfig::setTestOnCreate, BasePoolConfig::isTestOnCreate);
    }

    @Test
    void shouldAdaptTestOnRelease() {

        booleanTester(true, BaseObjectPoolConfig::setTestOnReturn, BasePoolConfig::isTestOnRelease);
        booleanTester(false, BaseObjectPoolConfig::setTestOnReturn, BasePoolConfig::isTestOnRelease);
    }

    static void booleanTester(boolean value, BiConsumer<GenericObjectPoolConfig<?>, Boolean> commonsConfigurer,
            Function<BoundedPoolConfig, Boolean> targetExtractor) {

        GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();

        commonsConfigurer.accept(config, value);
        BoundedPoolConfig result = CommonsPool2ConfigConverter.bounded(config);

        assertThat(targetExtractor.apply(result)).isEqualTo(value);
    }

}
