package io.atleon.application;

import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import io.atleon.core.Autostart;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractConfiguredAloStreamTest {

    @Test
    public void applicationReady_givenAutostartDisabled_expectsStreamToNotStart() {
        AloStreamConfig config = mock(AloStreamConfig.class);
        when(config.autostart()).thenReturn(Autostart.DISABLED);

        TestConfiguredAloStream stream = new TestConfiguredAloStream(new TestAloStream(), config);

        stream.applicationReady();

        assertEquals(AloStream.State.STOPPED, stream.state());
    }

    @Test
    public void applicationReady_givenAutostartEnabled_expectsStreamToStart() {
        AloStreamConfig config = mock(AloStreamConfig.class);
        when(config.autostart()).thenReturn(Autostart.ENABLED);

        TestConfiguredAloStream stream = new TestConfiguredAloStream(new TestAloStream(), config);

        stream.applicationReady();

        assertEquals(AloStream.State.STARTED, stream.state());
    }

    private static final class TestAloStream extends AloStream<AloStreamConfig> {

        @Override
        protected Disposable startDisposable(AloStreamConfig config) {
            return () -> {
            };
        }
    }

    private static final class TestConfiguredAloStream extends AbstractConfiguredAloStream<AloStreamConfig> {

        public TestConfiguredAloStream(AloStream<AloStreamConfig> stream, AloStreamConfig config) {
            super(stream, config);
        }
    }
}