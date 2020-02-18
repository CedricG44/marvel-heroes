package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Sorts.*;

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

        String dateFilter = "{\n" +
                "            $match : {\n" +
                "                \"identity.yearAppearance\" : {\n" +
                "                    \"$ne\": \"\"\n" +
                "                } \n" +
                "            }\n" +
                "        }";

        String groupByYear = "{ \n" +
                "            $group : { \n" +
                "                _id :  { \n" +
                "                    yearAppearance: \"$identity.yearAppearance\",\n" +
                "                    universe: \"$identity.universe\",\n" +
                "                },\n" +
                "                count: { $sum : 1 } \n" +
                "            }\n" +
                "        }";

        String groupByUniverse = "{\n" +
                "            $group : { \n" +
                "                _id :  \"$_id\",\n" +
                "                byUniverse: { \n" +
                "                    $push: { \n" +
                "                        universe:\"$_id.universe\",\n" +
                "                        count:\"$count\"\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }";

        String sort = "{ \n" +
                "            $sort : {\n" +
                "                \"_id.yearAppearance\" : 1\n" +
                "            }\n" +
                "        }";

        Document dateFilterDoc = Document.parse(dateFilter);
        Document groupByYearDoc = Document.parse(groupByYear);
        Document groupByUniverseDoc = Document.parse(groupByUniverse);
        Document sortDoc = Document.parse(sort);

        List<Document> pipeline = new ArrayList<Document>();
        pipeline.add(dateFilterDoc);
        pipeline.add(groupByYearDoc);
        pipeline.add(groupByUniverseDoc);
        pipeline.add(sortDoc);

        //Document document = Document.parse("{yearAppearance: \"$identity.yearAppearance\",universe: \"$identity.universe\"}");
        //Document byUniverseDoc = Document.parse("{ byUniverse : { universe : \"$_id.universe\", count : \"$count\" }}");
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(
                pipeline
                /*Arrays.asList(

                Aggregates.match(Filters.ne("identity.yearAppearance", "")),
                Aggregates.group(document, Accumulators.sum("count",1)),
                Aggregates.("", Accumulators.push("universe", "_id.universe"), Accumulators.push("count", "count"))
                Aggregates.sort(orderBy(ascending("_id.yearAppearance"))))*/
        ))
                .thenApply(documents -> documents.stream()
                        .map(Document::toJson)
                        .map(Json::parse)
                        .map(jsonNode -> {
                            System.out.println(jsonNode);
                            int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                            ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                            Iterator<JsonNode> elements = byUniverseNode.elements();
                            Iterable<JsonNode> iterable = () -> elements;
                            List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                    .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                    .collect(Collectors.toList());
                            return new YearAndUniverseStat(year, byUniverse);

                        })
                        .collect(Collectors.toList()));
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
