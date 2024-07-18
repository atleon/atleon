package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompositeAloStreamTest {

    @Test
    public void nCopies_givenExtensibleConfig_expectsUniquelyCopiedStreams() {
        int copyCount = 2;
        Set<String> streamNames = new LinkedHashSet<>();
        Set<String> customValues = new LinkedHashSet<>();
        Consumer<TestAloStreamConfig> configConsumer = it -> {
            streamNames.add(it.name());
            customValues.add(it.customValue());
        };

        AloStream<? super TestAloStreamConfig> stream =
            CompositeAloStream.nCopies(copyCount, () -> new TestAloStream(configConsumer));

        TestAloStreamConfig config = new TestAloStreamConfig("custom");
        stream.start(config);

        assertEquals(copyCount, streamNames.size());
        assertTrue(streamNames.stream().allMatch(it -> it.contains(config.name())));
        assertEquals(Collections.singleton(config.customValue()), customValues);
        assertEquals(AloStream.State.STARTED, stream.state());
    }

    public static class TestAloStreamConfig implements AloStreamConfig {

        private final String customValue;

        public TestAloStreamConfig(String customValue) {
            this.customValue = customValue;
        }

        public String customValue() {
            return customValue;
        }
    }

    public static class TestAloStream extends AloStream<TestAloStreamConfig> {

        private final Consumer<TestAloStreamConfig> configConsumer;

        public TestAloStream(Consumer<TestAloStreamConfig> configConsumer) {
            this.configConsumer = configConsumer;
        }

        @Override
        protected Disposable startDisposable(TestAloStreamConfig config) {
            configConsumer.accept(config);
            return Disposables.single();
        }
    }
}