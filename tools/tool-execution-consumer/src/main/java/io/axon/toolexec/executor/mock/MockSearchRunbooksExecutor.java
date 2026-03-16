package io.axon.toolexec.executor.mock;

import io.axon.toolexec.executor.ToolExecutor;

import java.util.List;
import java.util.Map;

public class MockSearchRunbooksExecutor implements ToolExecutor {

    @Override
    public String toolName() {
        return "search_runbooks";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) {
        String query = (String) input.getOrDefault("query", "");
        String category = (String) input.getOrDefault("category", "all");

        return Map.of(
                "results", List.of(
                        Map.of(
                                "title", "Runbook: " + query,
                                "category", category,
                                "content", "This is a mock runbook result for: " + query
                                        + ". In production, this queries the Qdrant RAG database.",
                                "score", 0.92
                        )
                ),
                "total_results", 1
        );
    }
}
