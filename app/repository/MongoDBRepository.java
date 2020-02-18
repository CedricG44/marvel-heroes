package repository;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        String query = String.format("{\"id\": \"%s\"}", heroId);
        Document document = Document.parse(query);
        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        return CompletableFuture.completedFuture(new ArrayList<>());
        /*List<Document> pipeline = new ArrayList<>();
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(Arrays.asList(
                Aggregates.group("$")
                Aggregates.group("byUniverse", Accumulators.sum("count",1))
        )))
                .thenApply(documents -> documents.stream()
                        .map(Document::toJson)
                        .map(Json::parse)
                        .map(jsonNode -> {
                            int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                            ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                            Iterator<JsonNode> elements = byUniverseNode.elements();
                            Iterable<JsonNode> iterable = () -> elements;
                            List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                    .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                    .collect(Collectors.toList());
                            return new YearAndUniverseStat(year, byUniverse);

                        })
                        .collect(Collectors.toList()));*/
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(Arrays.asList(
                Aggregates.unwind("$powers"),
                Aggregates.group("$powers", Accumulators.sum("count", 1)),
                Aggregates.sort(orderBy(descending("count"))),
                Aggregates.limit(top)
        ))).thenApply(documents -> documents.stream()
                .map(Document::toJson)
                .map(Json::parse)
                .map(jsonNode -> new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt()))
                .collect(Collectors.toList()));
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(Arrays.asList(
                Aggregates.group("$identity.universe", Accumulators.sum("count", 1))
        ))).thenApply(documents -> documents.stream()
                .map(Document::toJson)
                .map(Json::parse)
                .map(jsonNode -> new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt()))
                .collect(Collectors.toList()));
    }
}
