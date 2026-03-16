package io.axon.toolexec.executor.kafka;

import io.axon.toolexec.executor.ToolExecutor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.stream.Collectors;

public class DescribeTopicExecutor implements ToolExecutor {

    private final AdminClient adminClient;

    public DescribeTopicExecutor(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public String toolName() {
        return "describe_topic";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) throws Exception {
        String topicName = (String) input.get("topic_name");
        if (topicName == null || topicName.isBlank()) {
            return Map.of("error", "topic_name is required");
        }

        TopicDescription desc = adminClient
                .describeTopics(List.of(topicName)).allTopicNames().get().get(topicName);

        // Get topic configs
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<ConfigResource, org.apache.kafka.clients.admin.Config> configs =
                adminClient.describeConfigs(List.of(resource)).all().get();
        Collection<ConfigEntry> configEntries = configs.get(resource).entries();

        int partitionCount = desc.partitions().size();
        int replicationFactor = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();

        // Non-default configs as comma-separated string
        String configStr = configEntries.stream()
                .filter(e -> !e.isDefault() && e.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG)
                .sorted(Comparator.comparing(ConfigEntry::name))
                .map(e -> e.name() + "=" + e.value())
                .collect(Collectors.joining(","));

        // Build console output matching real kafka-topics --describe --topic <name>
        StringBuilder console = new StringBuilder();
        console.append("$ kafka-topics --bootstrap-server localhost:9092 --describe --topic ").append(topicName).append("\n");

        // Topic header line
        console.append("Topic: ").append(topicName)
                .append("\tTopicId: ").append(desc.topicId())
                .append("\tPartitionCount: ").append(partitionCount)
                .append("\tReplicationFactor: ").append(replicationFactor)
                .append("\tConfigs: ").append(configStr)
                .append("\n");

        // Partition lines
        for (TopicPartitionInfo p : desc.partitions()) {
            String leader = p.leader() != null ? String.valueOf(p.leader().id()) : "none";
            String replicas = p.replicas().stream().map(n -> String.valueOf(n.id())).collect(Collectors.joining(","));
            String isr = p.isr().stream().map(n -> String.valueOf(n.id())).collect(Collectors.joining(","));
            Set<Integer> isrIds = p.isr().stream().map(Node::id).collect(Collectors.toSet());
            String offline = p.replicas().stream()
                    .map(Node::id)
                    .filter(id -> !isrIds.contains(id))
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));

            console.append("\tTopic: ").append(topicName)
                    .append("\tPartition: ").append(p.partition())
                    .append("\tLeader: ").append(leader)
                    .append("\tReplicas: ").append(replicas)
                    .append("\tIsr: ").append(isr)
                    .append("\tOffline: ").append(offline)
                    .append("\n");
        }

        // Build structured result
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("console_output", console.toString().stripTrailing());
        result.put("topic", topicName);
        result.put("topic_id", desc.topicId().toString());
        result.put("partitions", partitionCount);
        result.put("replication_factor", replicationFactor);
        result.put("is_internal", desc.isInternal());

        if (!configStr.isEmpty()) {
            Map<String, String> configMap = new LinkedHashMap<>();
            configEntries.stream()
                    .filter(e -> !e.isDefault() && e.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG)
                    .sorted(Comparator.comparing(ConfigEntry::name))
                    .forEach(e -> configMap.put(e.name(), e.value()));
            result.put("configs", configMap);
        }

        return result;
    }
}
