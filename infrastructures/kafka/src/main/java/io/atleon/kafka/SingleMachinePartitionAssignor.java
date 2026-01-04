package io.atleon.kafka;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

/**
 * Consumer partition assignor that allocates all partitions for any given topic to a single
 * "machine". A "machine" is identified by a random, statically-allocated UUID, and therefore
 * machine identity is tied to the Java Virtual Machine lifecycle, so each JVM instance is a
 * unique machine. When multiple machines are involved in consumption, the "oldest" machine is
 * prioritized. Within a given machine, if there are multiple consumer instances, partitions are
 * balanced among those consumers (via round-robin).
 */
public final class SingleMachinePartitionAssignor extends AbstractPartitionAssignor {

    private static final MachineData MACHINE_DATA = MachineData.birth();

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return MACHINE_DATA.toByteBuffer();
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            Map<String, Integer> partitionCounts, Map<String, Subscription> subscriptionsByMemberId) {
        Map<String, List<MachineMemberData>> machineMemberDataByTopic = subscriptionsByMemberId.entrySet().stream()
                .flatMap(it -> streamTopicMachineMemberData(it.getKey(), it.getValue()))
                .collect(Collectors.groupingBy(
                        TopicMachineMemberData::topic,
                        Collectors.mapping(TopicMachineMemberData::machineMemberData, Collectors.toList())));

        Map<String, List<String>> memberIdsToAssignByTopic = machineMemberDataByTopic.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, it -> chooseMemberIdsToAssign(it.getValue())));

        Map<String, List<TopicPartition>> assignments = memberIdsToAssignByTopic.entrySet().stream()
                .flatMap(it -> assign(it.getKey(), partitionCounts.getOrDefault(it.getKey(), 0), it.getValue()))
                .collect(Collectors.groupingBy(
                        AssignedTopicPartition::memberId,
                        Collectors.mapping(AssignedTopicPartition::topicPartition, Collectors.toList())));

        // Every member ID must have an assignment, even if empty
        subscriptionsByMemberId.keySet().forEach(it -> assignments.putIfAbsent(it, Collections.emptyList()));

        return assignments;
    }

    @Override
    public String name() {
        return "singlemachine";
    }

    @Override
    public short version() {
        return 1;
    }

    private Stream<TopicMachineMemberData> streamTopicMachineMemberData(String memberId, Subscription subscription) {
        MachineData machineData = MachineData.fromByteBuffer(subscription.userData());
        return subscription.topics().stream().map(it -> new TopicMachineMemberData(it, machineData, memberId));
    }

    private List<String> chooseMemberIdsToAssign(List<MachineMemberData> machineMemberData) {
        Map<MachineData, List<String>> memberIdsByMachineData = machineMemberData.stream()
                .collect(Collectors.groupingBy(
                        MachineMemberData::machineData,
                        Collectors.mapping(MachineMemberData::memberId, Collectors.toList())));

        // Ensure deterministic ordering of member IDs
        memberIdsByMachineData.values().forEach(it -> it.sort(Comparator.naturalOrder()));

        return memberIdsByMachineData.keySet().stream()
                .min(Comparator.comparing(MachineData::birthTime).thenComparing(MachineData::id))
                .map(memberIdsByMachineData::get)
                .orElse(Collections.emptyList());
    }

    private Stream<AssignedTopicPartition> assign(String topic, int partitionCount, List<String> memberIds) {
        return IntStream.range(0, partitionCount)
                .mapToObj(it -> new AssignedTopicPartition(memberIds.get(it % memberIds.size()), topic, it));
    }

    private static final class MachineMemberData {

        private final MachineData machineData;

        private final String memberId;

        public MachineMemberData(MachineData machineData, String memberId) {
            this.machineData = machineData;
            this.memberId = memberId;
        }

        public MachineData machineData() {
            return machineData;
        }

        public String memberId() {
            return memberId;
        }
    }

    private static final class TopicMachineMemberData {

        private final String topic;

        private final MachineMemberData machineMemberData;

        public TopicMachineMemberData(String topic, MachineData machineData, String memberId) {
            this.topic = topic;
            this.machineMemberData = new MachineMemberData(machineData, memberId);
        }

        public String topic() {
            return topic;
        }

        public MachineMemberData machineMemberData() {
            return machineMemberData;
        }
    }

    private static final class AssignedTopicPartition {

        private final String memberId;

        private final TopicPartition topicPartition;

        public AssignedTopicPartition(String memberId, String topic, int partition) {
            this.memberId = memberId;
            this.topicPartition = new TopicPartition(topic, partition);
        }

        public String memberId() {
            return memberId;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }
    }
}
