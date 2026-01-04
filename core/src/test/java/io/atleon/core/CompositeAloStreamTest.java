package io.atleon.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;

class CompositeAloStreamTest {

    @Test
    public void nCopies_givenExtensibleConfig_expectsUniquelyCopiedStreams() {
        int copyCount = 2;
        Set<String> streamNames = new LinkedHashSet<>();
        Set<String> customValues1 = new LinkedHashSet<>();
        Set<String> customValues2 = new LinkedHashSet<>();
        Consumer<TestAloStreamConfig> configConsumer = it -> {
            streamNames.add(it.name());
            customValues1.add(it.customValue1());
            customValues2.add(it.customValue2());
        };

        AloStream<? super TestAloStreamConfig> stream =
                CompositeAloStream.nCopies(copyCount, () -> new TestAloStream(configConsumer));

        TestAloStreamConfig config = new TestAloStreamConfig("custom1", "custom2");
        stream.start(config);

        assertEquals(copyCount, streamNames.size());
        assertTrue(streamNames.stream().allMatch(it -> !it.equals(config.name()) && it.contains(config.name())));
        assertEquals(Collections.singleton(config.customValue1()), customValues1);
        assertEquals(Collections.singleton(config.customValue2()), customValues2);
        assertEquals(AloStream.State.STARTED, stream.state());
    }

    public static class TestAloStreamConfig implements AloStreamConfig {

        private final String customValue1;

        private final String customValue2;

        public TestAloStreamConfig(String customValue1, String customValue2) {
            this.customValue1 = customValue1;
            this.customValue2 = customValue2;
        }

        public String customValue1() {
            return customValue1;
        }

        public String customValue2() {
            return customValue2;
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
