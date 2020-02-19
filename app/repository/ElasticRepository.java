package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.Logger;
import play.libs.Json;
import play.libs.ws.WSClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Singleton
public class ElasticRepository {

    private static final Logger.ALogger LOGGER = Logger.of("ElasticRepository");
    private static final String HEROES_SEARCH_PATH = "/heroes/_search";

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }

    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        LOGGER.info("Search heroes");
        return wsClient.url(elasticConfiguration.uri + HEROES_SEARCH_PATH)
                .post(Json.parse(
                                "{\n" +
                                "  \"from\": " + size * (page - 1) + ",\n" +
                                "  \"size\": " + size + ",\n" +
                                "  \"query\": {\n" +
                                "    \"query_string\": {\n" +
                                "      \"fields\": [\n" +
                                "        \"name^4\",\n" +
                                "        \"aliases^3\",\n" +
                                "        \"secretIdentities^3\",\n" +
                                "        \"description^2\",\n" +
                                "        \"partners\"\n" +
                                "      ],\n" +
                                "      \"query\": \"" + (input.isEmpty() ? input + "*" : input + "~") + "\"\n" +
                                "    }\n" +
                                "  }\n" +
                                "}"))
                .thenApply(response -> {
                    final JsonNode hits = response.asJson().get("hits");
                    final List<SearchedHero> heroes = mapHeroesFromJson(response.asJson().get("hits"), "hits")
                            .collect(Collectors.toList());
                    final int total = hits.get("total").get("value").asInt();
                    return new PaginatedResults<>(total, page, Math.max(1, (int) Math.ceil((double) total / (double) size)), heroes);
                })
                .exceptionally(e -> new PaginatedResults<>(1, 1, 1, handleErrors(e)));
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        LOGGER.info("Suggest heroes");
        return wsClient.url(elasticConfiguration.uri + HEROES_SEARCH_PATH)
                .post(Json.parse(
                                "{\n" +
                                "  \"suggest\": {\n" +
                                "    \"suggestion\": {\n" +
                                "      \"prefix\": \"" + input + "\",\n" +
                                "      \"completion\": {\n" +
                                "        \"field\": \"suggest\"\n" +
                                "      }\n" +
                                "    }\n" +
                                "  }\n" +
                                "}"))
                .thenApply(response -> {
                    final Spliterator<JsonNode> iterator = response.asJson().get("suggest").get("suggestion").spliterator();
                    return StreamSupport.stream(iterator, false)
                            .flatMap(n -> mapHeroesFromJson(n, "options"))
                            .collect(Collectors.toList());
                })
                .exceptionally(this::handleErrors);
    }

    private Stream<SearchedHero> mapHeroesFromJson(final JsonNode node, final String nodeKey) {
        return StreamSupport.stream(node.get(nodeKey).spliterator(), false)
                .map(e -> {
                    final JsonNode n = e.get("_source");
                    ((ObjectNode) n).put("id", e.get("_id").textValue());
                    return SearchedHero.fromJson(n);
                });
    }

    private List<SearchedHero> handleErrors(final Throwable e) {
        LOGGER.error("Error while contacting Elasticsearch: " + e.getMessage(), e);
        return Collections.emptyList();
    }
}
