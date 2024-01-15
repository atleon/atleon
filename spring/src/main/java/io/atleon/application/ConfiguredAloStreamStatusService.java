package io.atleon.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConfiguredAloStreamStatusService implements AloStreamStatusService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredAloStreamStatusService.class);

    private final SortedMap<String, ConfiguredAloStream> streamsByName;

    public ConfiguredAloStreamStatusService(Collection<? extends ConfiguredAloStream> streams) {
        this.streamsByName = streams.stream().collect(Collectors.toMap(
            ConfiguredAloStream::name,
            Function.identity(),
            ConfiguredAloStreamStatusService::logConflictingStreamsAndChooseFirst,
            TreeMap::new
        ));
    }

    @Override
    public Collection<AloStreamStatusDto> getAllStatuses() {
        return streamsByName.values().stream().map(this::createDto).collect(Collectors.toList());
    }

    @Override
    public Optional<AloStreamStatusDto> getStatus(String name) {
        return Optional.ofNullable(streamsByName.get(name)).map(this::createDto);
    }

    @Override
    public Optional<AloStreamStatusDto> start(String name) {
        Optional<ConfiguredAloStream> foundStream = Optional.ofNullable(streamsByName.get(name));
        foundStream.ifPresent(ConfiguredAloStream::start);
        return foundStream.map(this::createDto);
    }

    @Override
    public Optional<AloStreamStatusDto> stop(String name) {
        Optional<ConfiguredAloStream> foundStream = Optional.ofNullable(streamsByName.get(name));
        foundStream.ifPresent(ConfiguredAloStream::stop);
        return foundStream.map(this::createDto);
    }

    private AloStreamStatusDto createDto(ConfiguredAloStream stream) {
        return new AloStreamStatusDto(stream.name(), stream.state().name());
    }

    private static ConfiguredAloStream
    logConflictingStreamsAndChooseFirst(ConfiguredAloStream first, ConfiguredAloStream second) {
        LOGGER.warn("Conflicting Streams! Choosing first. name={} first={} second={}", first.name(), first, second);
        return first;
    }
}
