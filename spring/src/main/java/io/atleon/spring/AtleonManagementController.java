package io.atleon.spring;

import io.atleon.application.AloStreamStatusDto;
import io.atleon.application.AloStreamStatusService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RestController
@RequestMapping(path = "/${atleon.management.rest.path:atleonManagement}", produces = MediaType.APPLICATION_JSON_VALUE)
public class AtleonManagementController {

    private final AloStreamStatusService streamStatusService;

    AtleonManagementController(AloStreamStatusService streamStatusService) {
        this.streamStatusService = streamStatusService;
    }

    @Operation(summary = "Get statuses of all Streams")
    @GetMapping("streams")
    public ResponseEntity<Collection<AloStreamStatusDto>> getAllStreamStatuses() {
        return ResponseEntity.ok(streamStatusService.getAllStatuses());
    }

    @Operation(summary = "Get status of a single Stream")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Stream found"),
        @ApiResponse(responseCode = "404", description = "Stream not found")
    })
    @GetMapping("streams/{name}")
    public ResponseEntity<AloStreamStatusDto> getStreamStatus(@PathVariable("name") String name) {
        return ResponseEntity.of(streamStatusService.getStatus(name));
    }

    @Operation(summary = "Start a Stream")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Stream found and started"),
        @ApiResponse(responseCode = "404", description = "Stream not found")
    })
    @PutMapping("streams/{name}/start")
    public ResponseEntity<AloStreamStatusDto> startStream(@PathVariable("name") String name) {
        return ResponseEntity.of(streamStatusService.start(name));
    }

    @Operation(summary = "Stop a Stream")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Stream found and stopped"),
        @ApiResponse(responseCode = "404", description = "Stream not found")
    })
    @PutMapping("streams/{name}/stop")
    public ResponseEntity<AloStreamStatusDto> stopStream(@PathVariable("name") String name) {
        return ResponseEntity.of(streamStatusService.stop(name));
    }
}
