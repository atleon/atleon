package io.atleon.spring;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Describes the status of a registered Stream")
public final class AloStreamStatusDto {

    @Schema(description = "The name of the registered Stream")
    private final String name;

    @Schema(description = "The state of the registered Stream. Typically STARTED or STOPPED.", example = "STARTED")
    private final String state;

    public AloStreamStatusDto(String name, String state) {
        this.name = name;
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }
}
