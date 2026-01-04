package io.atleon.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class MachineDataTest {

    @Test
    public void fromByteBuffer_givenSerializedMachineData_expectsEqualDeserializedMachineData() {
        MachineData machineData = MachineData.birth();

        ByteBuffer byteBuffer = machineData.toByteBuffer();

        MachineData result = MachineData.fromByteBuffer(byteBuffer);

        assertEquals(machineData, result);
    }
}
