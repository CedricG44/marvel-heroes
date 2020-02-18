package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                 .post(Json.parse(
                         "{\n" +
                         "  \"from\": " + size * (page - 1) + ",\n" +
                         "  \"size\": " + size + ",\n" +
                         "  \"query\": {\n" +
                         "    \"query_string\": {\n" +
                         "      \"default_field\": \"name.keyword\",\n" +
                         "      \"query\": \"*" + input.replaceAll(" ", "*") + "*\"\n" +
                         "    }\n" +
                         "  }\n" +
                         "}"))
                 .thenApply(response -> {
                     final List<SearchedHero> heroes = new ArrayList<>();
                     final JsonNode hits = response.asJson().get("hits");

                     // Création des héros
                     hits.get("hits").elements().forEachRemaining(e -> {
                         final JsonNode node = e.get("_source");
                         ((ObjectNode) node).put("id", e.get("_id").textValue());
                         heroes.add(SearchedHero.fromJson(node));
                     });

                     final int total = hits.get("total").get("value").asInt();
                     return new PaginatedResults<>(total, page, (int) Math.ceil((double) total / (double) size), heroes);
                 });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return CompletableFuture.completedFuture(Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan()));
        // TODO
        // return wsClient.url(elasticConfiguration.uri + "...")
        //         .post(Json.parse("{ ... }"))
        //         .thenApply(response -> {
        //             return ...
        //         });
    }
}
