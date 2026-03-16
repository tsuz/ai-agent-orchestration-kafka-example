package io.axon.toolexec.executor.kafka;

import io.axon.toolexec.executor.ToolExecutor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

public class GetConsumerLagExecutor implements ToolExecutor {

    private final AdminClient adminClient;

    public GetConsumerLagExecutor(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public String toolName() {
        return "get_consumer_lag";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) throws Exception {
        String groupId = (String) input.get("group_id");
        if (groupId == null || groupId.isBlank()) {
            return Map.of("error", "group_id is required");
        }

        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get();

        if (offsets.isEmpty()) {
            return Map.of(
                    "console_output", "No committed offsets found for group '" + groupId + "'.",
                    "group_id", groupId,
                    "error", "No committed offsets found"
            );
        }

        Map<TopicPartition, OffsetSpec> endOffsetRequest = offsets.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                adminClient.listOffsets(endOffsetRequest).all().get();

        // Build console output
        StringBuilder console = new StringBuilder();
        console.append("$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ").append(groupId).append("\n\n");
        console.append("TOPIC                                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG\n");

        long totalLag = 0;
        for (var entry : offsets.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().toString()))
                .collect(Collectors.toList())) {
            TopicPartition tp = entry.getKey();
            long committed = entry.getValue().offset();
            long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : -1;
            long lag = endOffset >= 0 ? endOffset - committed : 0;
            totalLag += lag;

            console.append(String.format("%-40s %-10d %-15d %-15d %d\n",
                    tp.topic(), tp.partition(), committed, endOffset, lag));
        }
        console.append(String.format("\nTotal lag: %d", totalLag));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("console_output", console.toString());
        result.put("group_id", groupId);
        result.put("total_lag", totalLag);
        result.put("status", totalLag == 0 ? "caught_up" : totalLag < 1000 ? "minor_lag" : "significant_lag");
        return result;
    }
}
