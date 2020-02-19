package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.Logger;
import play.libs.Json;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

@Singleton
public class MongoDBRepository {

    private static final Logger.ALogger LOGGER = Logger.of("MongoDBRepository");

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }

    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        LOGGER.info("Retrieved hero by id");
        final String query = String.format("{\"id\": \"%s\"}", heroId);
        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(Document.parse(query)).first())
                .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson))
                .exceptionally(e -> {
                    handleErrors(e);
                    return Optional.empty();
                });
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        LOGGER.info("Retrieved count by year and universe");
        final String dateFilter = "{\n" +
                "            $match: {\n" +
                "                \"identity.yearAppearance\": {\n" +
                "                    \"$ne\": \"\"\n" +
                "                }\n" +
                "            }\n" +
                "        }";

        final String groupByYear = "{\n" +
                "            $group: {\n" +
                "                _id:  {\n" +
                "                    yearAppearance: \"$identity.yearAppearance\",\n" +
                "                    universe: \"$identity.universe\",\n" +
                "                },\n" +
                "                count: { $sum : 1 }\n" +
                "            }\n" +
                "        }";

        final String groupByUniverse = "{\n" +
                "            $group: { \n" +
                "                _id:  \"$_id\",\n" +
                "                byUniverse: {\n" +
                "                    $push: {\n" +
                "                        universe:\"$_id.universe\",\n" +
                "                        count:\"$count\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }";

        final String sort = "{\n" +
                "            $sort : {\n" +
                "                \"_id.yearAppearance\" : 1\n" +
                "            }\n" +
                "        }";

        final List<Document> pipeline = new ArrayList<>();
        pipeline.add(Document.parse(dateFilter));
        pipeline.add(Document.parse(groupByYear));
        pipeline.add(Document.parse(groupByUniverse));
        pipeline.add(Document.parse(sort));

        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> documents.stream()
                        .map(Document::toJson)
                        .map(Json::parse)
                        .map(jsonNode -> {
                            int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                            final Iterable<JsonNode> iterable = () -> jsonNode.findPath("byUniverse").elements();
                            final List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                    .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                    .collect(Collectors.toList());
                            return new YearAndUniverseStat(year, byUniverse);

                        })
                        .collect(Collectors.toList()))
                .exceptionally(e -> {
                    handleErrors(e);
                    return Collections.emptyList();
                });
    }

    public CompletionStage<List<ItemCount>> topPowers(int top) {
        LOGGER.info("Retrieved top powers");
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(
                Arrays.asList(
                        Aggregates.unwind("$powers"),
                        Aggregates.group("$powers", Accumulators.sum("count", 1)),
                        Aggregates.sort(orderBy(descending("count"))),
                        Aggregates.limit(top))))
                .thenApply(documents -> documents.stream()
                        .map(Document::toJson)
                        .map(Json::parse)
                        .map(jsonNode -> new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt()))
                        .collect(Collectors.toList()))
                .exceptionally(e -> {
                    handleErrors(e);
                    return Collections.emptyList();
                });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        LOGGER.info("Retrieved by universe");
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(
                Collections.singletonList(
                        Aggregates.group("$identity.universe", Accumulators.sum("count", 1)))))
                .thenApply(documents -> documents.stream()
                        .map(Document::toJson)
                        .map(Json::parse)
                        .map(jsonNode -> new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt()))
                        .collect(Collectors.toList()))
                .exceptionally(e -> {
                    handleErrors(e);
                    return Collections.emptyList();
                });
    }

    private void handleErrors(final Throwable e) {
        LOGGER.error("Error while contacting MongoDB: " + e.getMessage(), e);
    }
}
