package io.atleon.spring;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import reactor.core.Disposable;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SpringAloStreamTest {

    @Test
    public void getProperty_givenExplicitAndDefaultProperties_expectsReturnedValues() {
        Environment environment = mock(Environment.class);
        when(environment.getProperty(eq("stream.defaults.concurrency"))).thenReturn("2");
        when(environment.getProperty(eq("stream.defaults.batch.duration"))).thenReturn("PT5S");
        when(environment.getProperty(eq("stream.test.concurrency"))).thenReturn("4");

        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getEnvironment()).thenReturn(environment);

        TestStream stream = new TestStream(context);

        assertEquals(4, stream.getStreamProperty("concurrency", Double::valueOf, 1D));
        assertEquals(1, stream.getStreamProperty("batch.size", Integer::valueOf, 1));
        assertEquals(Duration.ofSeconds(5), stream.getStreamProperty("batch.duration", Duration::parse, Duration.ZERO));
    }

    private static final class TestStream extends SpringAloStream {

        public TestStream(ApplicationContext context) {
            super(context);
        }

        @Override
        protected Disposable startDisposable() {
            return () -> {};
        }
    }
}