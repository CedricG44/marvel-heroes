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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private static Logger.ALogger logger = Logger.of("ElasticRepository");

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }

    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        logger.info("Search heroes");
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(
                                "{\n" +
                                "  \"from\": " + size * (page - 1) + ",\n" +
                                "  \"size\": " + size + ",\n" +
                                "  \"query\": {\n" +
                                "    \"query_string\": {\n" +
                                "      \"fields\": [\n" +
                                "        \"name.keyword^4\",\n" +
                                "        \"aliases.keyword^3\",\n" +
                                "        \"secretIdentities.keyword^3\",\n" +
                                "        \"description.keyword^2\",\n" +
                                "        \"partners.keyword\"\n" +
                                "      ],\n" +
                                "      \"query\": \"*" + input.replaceAll(" ", "*") + "*\"\n" +
                                "    }\n" +
                                "  }\n" +
                                "}"))
                .thenApply(response -> {
                    final JsonNode hits = response.asJson().get("hits");
                    final List<SearchedHero> heroes = mapHeroesFromJson(response.asJson().get("hits"));
                    final int total = hits.get("total").get("value").asInt();
                    return new PaginatedResults<>(total, page, (int) Math.ceil((double) total / (double) size), heroes);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        logger.info("Suggest heroes");
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(
                                "{\n" +
                                "  \"size\": 7,\n" +
                                "  \"query\": {\n" +
                                "    \"query_string\": {\n" +
                                "      \"fields\": [\n" +
                                "        \"name.keyword^4\",\n" +
                                "        \"aliases.keyword^3\",\n" +
                                "        \"secretIdentities.keyword^3\",\n" +
                                "        \"description.keyword^2\",\n" +
                                "        \"partners.keyword\"\n" +
                                "      ],\n" +
                                "      \"query\": \"*" + input.replaceAll(" ", "*") + "*\"\n" +
                                "    }\n" +
                                "  }\n" +
                                "}"))
                .thenApply(response -> mapHeroesFromJson(response.asJson().get("hits")));
    }

    private List<SearchedHero> mapHeroesFromJson(final JsonNode hits) {
        final List<SearchedHero> heroes = new ArrayList<>();

        // Création des héros
        hits.get("hits").elements().forEachRemaining(e -> {
            final JsonNode node = e.get("_source");
            ((ObjectNode) node).put("id", e.get("_id").textValue());
            heroes.add(SearchedHero.fromJson(node));
        });
        return heroes;
    }
}
