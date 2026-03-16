package io.axon.toolexec.executor.mock;

import io.axon.toolexec.executor.ToolExecutor;

import java.util.Map;

public class MockLookupKafkaConfigExecutor implements ToolExecutor {

    private static final Map<String, Map<String, String>> CONFIG_DB = Map.of(
            "retention.ms", Map.of(
                    "description", "How long to retain log segments for a topic before deletion.",
                    "default", "604800000 (7 days)",
                    "our_setting", "604800000 (7 days), 2592000000 (30 days) for audit topics",
                    "scope", "topic",
                    "impact", "Lowering reduces disk usage but data is permanently lost. Increasing uses more disk."
            ),
            "min.insync.replicas", Map.of(
                    "description", "Minimum number of replicas that must acknowledge a write for it to be considered successful (when acks=all).",
                    "default", "1",
                    "our_setting", "2",
                    "scope", "broker/topic",
                    "impact", "Higher values increase durability but reduce availability. With replication-factor=3 and min.insync.replicas=2, the cluster tolerates 1 broker failure."
            ),
            "max.poll.records", Map.of(
                    "description", "Maximum number of records returned in a single call to poll().",
                    "default", "500",
                    "our_setting", "500 (default), 1 for think-consumer (LLM calls)",
                    "scope", "consumer",
                    "impact", "Lower values reduce per-poll processing time but increase poll frequency overhead."
            ),
            "max.poll.interval.ms", Map.of(
                    "description", "Maximum time between poll() invocations before consumer is considered dead and rebalance is triggered.",
                    "default", "300000 (5 minutes)",
                    "our_setting", "300000",
                    "scope", "consumer",
                    "impact", "Too low causes unnecessary rebalances. Too high delays detection of stuck consumers."
            )
    );

    @Override
    public String toolName() {
        return "lookup_kafka_config";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) {
        String configKey = (String) input.getOrDefault("config_key", "");

        Map<String, String> entry = CONFIG_DB.get(configKey);
        if (entry != null) {
            return Map.of(
                    "config_key", configKey,
                    "found", true,
                    "description", entry.get("description"),
                    "default_value", entry.get("default"),
                    "our_setting", entry.get("our_setting"),
                    "scope", entry.get("scope"),
                    "impact", entry.get("impact")
            );
        }

        return Map.of(
                "config_key", configKey,
                "found", false,
                "message", "Configuration key '" + configKey + "' not found in the reference database. "
                        + "Check the Apache Kafka documentation for details."
        );
    }
}
