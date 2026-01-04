package io.atleon.core;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class ErrorDelegatorTest {

    @Test
    public void sendingDelegationResultsInSuccessIfResultIsEmpty() {
        ErrorDelegator.Sending<String, String> delegator = ErrorDelegator.sending(__ -> Mono.empty());

        assertNull(delegator.apply("data", new UnsupportedOperationException()).block());
    }

    @Test
    public void sendingDelegationResultsInSuccessIfResultIsSuccessful() {
        ErrorDelegator.Sending<String, String> delegator = ErrorDelegator.sending(__ -> Mono.just(Optional::empty));

        assertNotNull(
                delegator.apply("data", new UnsupportedOperationException()).block());
    }

    @Test
    public void sendingDelegationResultsInErrorIfPredicateDoesNotMatch() {
        ErrorDelegator.Sending<String, String> delegator = ErrorDelegator.<String>sending(__ -> Mono.empty())
                .errorMustMatch(IllegalArgumentException.class::isInstance);

        assertThrows(UnsupportedOperationException.class, () -> delegator
                .apply("data", new UnsupportedOperationException())
                .block());
    }

    @Test
    public void sendingDelegationResultsInSenderErrorIfItFails() {
        ErrorDelegator.Sending<String, String> delegator =
                ErrorDelegator.sending(__ -> Mono.error(new IllegalArgumentException()));

        assertThrows(IllegalArgumentException.class, () -> delegator
                .apply("data", new UnsupportedOperationException())
                .block());
    }

    @Test
    public void sendingDelegationResultsInSenderErrorIfResultsIsFailure() {
        ErrorDelegator.Sending<String, String> delegator =
                ErrorDelegator.sending(__ -> Mono.just(() -> Optional.of(new IllegalArgumentException())));

        assertThrows(IllegalArgumentException.class, () -> delegator
                .apply("data", new UnsupportedOperationException())
                .block());
    }
}
