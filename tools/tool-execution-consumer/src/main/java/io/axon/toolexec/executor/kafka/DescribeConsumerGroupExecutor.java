package io.axon.toolexec.executor.kafka;

import io.axon.toolexec.executor.ToolExecutor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

public class DescribeConsumerGroupExecutor implements ToolExecutor {

    private final AdminClient adminClient;

    public DescribeConsumerGroupExecutor(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public String toolName() {
        return "describe_consumer_group";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) throws Exception {
        String groupId = (String) input.get("group_id");
        if (groupId == null || groupId.isBlank()) {
            return Map.of("error", "group_id is required");
        }

        ConsumerGroupDescription desc = adminClient
                .describeConsumerGroups(List.of(groupId)).all().get().get(groupId);

        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> endOffsetRequest = offsets.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                adminClient.listOffsets(endOffsetRequest).all().get();

        // Build console output like: kafka-consumer-groups.sh --describe --group <id>
        StringBuilder console = new StringBuilder();
        console.append("$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ").append(groupId).append("\n\n");
        console.append(String.format("GROUP                          STATE           COORDINATOR     MEMBERS  ASSIGNMENT\n"));
        console.append(String.format("%-30s %-15s %-15s %-8d %s\n\n",
                groupId,
                desc.state().toString(),
                desc.coordinator().host() + ":" + desc.coordinator().port(),
                desc.members().size(),
                desc.partitionAssignor()));

        // Members
        if (!desc.members().isEmpty()) {
            console.append("MEMBER-ID                                                        CLIENT-ID                HOST            PARTITIONS\n");
            for (var m : desc.members()) {
                String memberId = m.consumerId().length() > 60
                        ? m.consumerId().substring(0, 57) + "..."
                        : m.consumerId();
                String parts = m.assignment().topicPartitions().stream()
                        .map(tp -> tp.topic() + "-" + tp.partition())
                        .sorted()
                        .collect(Collectors.joining(", "));
                console.append(String.format("%-64s %-24s %-15s %s\n",
                        memberId, m.clientId(), m.host(), parts));
            }
            console.append("\n");
        }

        // Partition offsets and lag
        console.append("TOPIC                                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG\n");
        long totalLag = 0;
        for (var entry : offsets.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().toString()))
                .collect(Collectors.toList())) {
            TopicPartition tp = entry.getKey();
            long committed = entry.getValue().offset();
            long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : -1;
            long lag = endOffset >= 0 ? endOffset - committed : -1;
            if (lag > 0) totalLag += lag;

            console.append(String.format("%-40s %-10d %-15d %-15d %d\n",
                    tp.topic(), tp.partition(), committed, endOffset, Math.max(lag, 0)));
        }
        console.append(String.format("\nTotal lag: %d", totalLag));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("console_output", console.toString());
        result.put("group_id", groupId);
        result.put("state", desc.state().toString());
        result.put("total_lag", totalLag);
        return result;
    }
}
