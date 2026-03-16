package io.axon.toolexec.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axon.toolexec.config.AppConfig;
import io.axon.toolexec.executor.kafka.*;
import io.axon.toolexec.executor.mock.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Registry that maps tool names to their executor implementations.
 * Kafka ops tools (list_consumer_groups, describe_consumer_group, list_topics,
 * get_consumer_lag) always use live AdminClient.
 * Knowledge-base tools use mock or HTTP executors based on MOCK_MODE.
 */
public class ToolExecutorRegistry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ToolExecutorRegistry.class);

    private final Map<String, ToolExecutor> executors = new HashMap<>();
    private final AdminClient adminClient;

    public ToolExecutorRegistry(ObjectMapper mapper) {
        // Shared AdminClient for all Kafka ops tools
        this.adminClient = createAdminClient();

        // Live Kafka ops tools
        register(new ListConsumerGroupsExecutor(adminClient));
        register(new DescribeConsumerGroupExecutor(adminClient));
        register(new ListTopicsExecutor(adminClient));
        register(new GetConsumerLagExecutor(adminClient));
        register(new DescribeTopicExecutor(adminClient));

        // Knowledge-base tools
        if (AppConfig.MOCK_MODE) {
            log.info("Knowledge-base tools running in MOCK mode");
            register(new MockSearchRunbooksExecutor());
            register(new MockLookupKafkaConfigExecutor());
        } else {
            log.info("Knowledge-base tools running in HTTP mode");
            register(new HttpToolExecutor("search_runbooks", AppConfig.RUNBOOKS_URL, mapper));
            register(new HttpToolExecutor("lookup_kafka_config", AppConfig.KAFKA_CONFIG_URL, mapper));
        }

        log.info("Registered {} tool executors: {}", executors.size(), executors.keySet());
    }

    public void register(ToolExecutor executor) {
        executors.put(executor.toolName(), executor);
    }

    public ToolExecutor get(String toolName) {
        return executors.get(toolName);
    }

    public boolean has(String toolName) {
        return executors.containsKey(toolName);
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
            log.info("AdminClient closed");
        }
    }

    private static AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return AdminClient.create(props);
    }
}
