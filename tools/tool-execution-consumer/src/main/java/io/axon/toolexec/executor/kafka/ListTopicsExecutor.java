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

public class ListTopicsExecutor implements ToolExecutor {

    private final AdminClient adminClient;

    public ListTopicsExecutor(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public String toolName() {
        return "list_topics";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) throws Exception {
        Set<String> topicNames = adminClient.listTopics().names().get();

        Map<String, TopicDescription> descriptions = adminClient
                .describeTopics(topicNames).allTopicNames().get();

        List<TopicDescription> sorted = descriptions.values().stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .collect(Collectors.toList());

        // Fetch configs for all topics
        List<ConfigResource> resources = sorted.stream()
                .map(td -> new ConfigResource(ConfigResource.Type.TOPIC, td.name()))
                .collect(Collectors.toList());
        Map<ConfigResource, org.apache.kafka.clients.admin.Config> allConfigs =
                adminClient.describeConfigs(resources).all().get();

        StringBuilder console = new StringBuilder();
        console.append("$ kafka-topics --bootstrap-server localhost:9092 --describe\n");

        for (TopicDescription td : sorted) {
            int partitionCount = td.partitions().size();
            int replication = td.partitions().isEmpty() ? 0 : td.partitions().get(0).replicas().size();

            // Get non-default configs
            ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, td.name());
            String configStr = "";
            if (allConfigs.containsKey(res)) {
                configStr = allConfigs.get(res).entries().stream()
                        .filter(e -> !e.isDefault() && e.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG)
                        .sorted(Comparator.comparing(ConfigEntry::name))
                        .map(e -> e.name() + "=" + e.value())
                        .collect(Collectors.joining(","));
            }

            // Topic header line — matches real kafka-topics output
            console.append("Topic: ").append(td.name())
                    .append("\tTopicId: ").append(td.topicId())
                    .append("\tPartitionCount: ").append(partitionCount)
                    .append("\tReplicationFactor: ").append(replication)
                    .append("\tConfigs: ").append(configStr.isEmpty() ? "" : configStr)
                    .append("\n");

            // Partition lines
            for (TopicPartitionInfo p : td.partitions()) {
                String leader = p.leader() != null ? String.valueOf(p.leader().id()) : "none";
                String replicas = p.replicas().stream().map(n -> String.valueOf(n.id())).collect(Collectors.joining(","));
                String isr = p.isr().stream().map(n -> String.valueOf(n.id())).collect(Collectors.joining(","));
                Set<Integer> isrIds = p.isr().stream().map(Node::id).collect(Collectors.toSet());
                String offline = p.replicas().stream()
                        .map(Node::id)
                        .filter(id -> !isrIds.contains(id))
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));

                console.append("\tTopic: ").append(td.name())
                        .append("\tPartition: ").append(p.partition())
                        .append("\tLeader: ").append(leader)
                        .append("\tReplicas: ").append(replicas)
                        .append("\tIsr: ").append(isr)
                        .append("\tOffline: ").append(offline)
                        .append("\n");
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("console_output", console.toString().stripTrailing());
        result.put("total_topics", sorted.size());
        return result;
    }
}
