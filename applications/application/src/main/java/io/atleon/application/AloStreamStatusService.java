package io.atleon.application;

import java.util.Collection;
import java.util.Optional;

public interface AloStreamStatusService {
    Collection<AloStreamStatusDto> getAllStatuses();

    Optional<AloStreamStatusDto> getStatus(String name);

    Optional<AloStreamStatusDto> start(String name);

    Optional<AloStreamStatusDto> stop(String name);
}
