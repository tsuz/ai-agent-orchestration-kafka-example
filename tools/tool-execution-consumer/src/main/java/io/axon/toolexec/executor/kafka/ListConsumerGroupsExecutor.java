package io.axon.toolexec.executor.kafka;

import io.axon.toolexec.executor.ToolExecutor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;

import java.util.*;
import java.util.stream.Collectors;

public class ListConsumerGroupsExecutor implements ToolExecutor {

    private final AdminClient adminClient;

    public ListConsumerGroupsExecutor(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public String toolName() {
        return "list_consumer_groups";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) throws Exception {
        Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups().all().get();

        List<String> groupIds = listings.stream()
                .map(ConsumerGroupListing::groupId)
                .sorted()
                .collect(Collectors.toList());

        // Console output like: kafka-consumer-groups.sh --list
        StringBuilder console = new StringBuilder();
        console.append("$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list\n");
        for (String gid : groupIds) {
            console.append(gid).append("\n");
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("console_output", console.toString().stripTrailing());
        result.put("total_groups", groupIds.size());
        result.put("groups", groupIds);
        return result;
    }
}
