package io.atleon.micrometer.observation;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.micrometer.observation.Observation;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ObservingAloTest {

    private final TestObservationRegistry registry = TestObservationRegistry.create();

    @Test
    public void start_givenAcknowledgedAlo_expectsStartedAndStoppedObservation() {
        TestAlo<String> alo = new TestAlo<>("I said what what");
        ObservingAlo<String> observingAlo = start(alo);

        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("test")
                .that()
                .hasBeenStarted()
                .isNotStopped();

        Alo.acknowledge(observingAlo);

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("test")
                .that()
                .hasBeenStopped()
                .doesNotHaveError();
    }

    @Test
    public void runInContext_givenRunnable_expectsObservationActiveInScope() {
        TestAlo<String> alo = new TestAlo<>("I said what what");
        ObservingAlo<String> observingAlo = start(alo);

        observingAlo.runInContext(() -> assertSame(observingAlo.observation, registry.getCurrentObservation()));
    }

    @Test
    public void supplyInContext_givenSupplier_expectsObservationActiveInScope() {
        TestAlo<String> alo = new TestAlo<>("I said what what");
        ObservingAlo<String> observingAlo = start(alo);

        Boolean observationActive =
                observingAlo.supplyInContext(() -> registry.getCurrentObservation() == observingAlo.observation);

        assertEquals(Boolean.TRUE, observationActive);
    }

    @Test
    public void map_givenMapper_expectsObservationActiveInScope() {
        TestAlo<String> alo = new TestAlo<>("I said what what");
        ObservingAlo<String> observingAlo = start(alo);

        observingAlo.map(string -> {
            assertSame(observingAlo.observation, registry.getCurrentObservation());
            return string.toUpperCase();
        });
    }

    @Test
    public void getAcknowledger_givenMappedAlo_expectsSingleAcknowledgementAndStoppedObservation() {
        TestAlo<String> alo = new TestAlo<>("I said what what");
        ObservingAlo<String> observingAlo = start(alo);

        Alo<String> mapped = observingAlo.map(string -> string.substring(0, 11)).map(String::toUpperCase);
        Alo.acknowledge(mapped);

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("test")
                .that()
                .hasBeenStopped()
                .doesNotHaveError();
    }

    @Test
    public void getAcknowledger_givenPublishedAlo_expectsSingleAcknowledgementAndStoppedObservation() {
        TestAlo<String> alo = new TestAlo<>("I said what what");
        ObservingAlo<String> observingAlo = start(alo);

        AloFactory<String> factory = observingAlo.propagator();
        Alo<String> published = factory.create(
                observingAlo.get().toUpperCase(), observingAlo.getAcknowledger(), observingAlo.getNacknowledger());
        Alo.acknowledge(published);

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("test")
                .that()
                .hasBeenStopped()
                .doesNotHaveError();
    }

    @Test
    public void getNacknowledger_givenNacknowledgedAlo_expectsErrorRecordedAndStoppedObservation() {
        TestAlo<String> alo = new TestAlo<>("I said what what");
        ObservingAlo<String> observingAlo = start(alo);

        RuntimeException error = new IllegalStateException("Boom");
        Alo.nacknowledge(observingAlo, error);

        assertEquals(0, alo.acknowledgeCount());
        assertEquals(1, alo.nacknowledgeCount());
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("test")
                .that()
                .hasBeenStopped()
                .hasError(error);
    }

    @Test
    public void fanInPropagator_givenFannedInAlos_expectsParentedFanInObservation() {
        TestAlo<String> alo1 = new TestAlo<>("I said what what");
        TestAlo<String> alo2 = new TestAlo<>("in the you know where");

        ObservingAlo<String> observingAlo1 = start(alo1);
        ObservingAlo<String> observingAlo2 = start(alo2);

        AloFactory<List<String>> factory = observingAlo1.fanInPropagator(Arrays.asList(observingAlo1, observingAlo2));
        Alo<List<String>> fannedIn = factory.create(Arrays.asList(alo1.get(), alo2.get()), () -> {}, error -> {});
        Alo.acknowledge(fannedIn);

        // The two primary observations, plus the fan-in observation
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(3)
                .hasNumberOfObservationsWithNameEqualTo("test", 2)
                .hasNumberOfObservationsWithNameEqualTo("atleon.fan.in", 1)
                .hasObservationWithNameEqualTo("atleon.fan.in")
                .that()
                .hasBeenStarted()
                .hasBeenStopped()
                .hasParentObservationEqualTo(observingAlo1.observation);
    }

    private ObservingAlo<String> start(TestAlo<String> alo) {
        Observation observation = Observation.createNotStarted("test", registry);
        return ObservingAlo.start(alo, registry, observation);
    }
}
