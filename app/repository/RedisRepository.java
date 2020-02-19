package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import models.StatItem;
import models.TopStatItem;
import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static final Logger.ALogger LOGGER = Logger.of("RedisRepository");
    private static final String TOP_HEROES_KEY = "top-heroes";
    private static final String VIEWED_HEROES_KEY = "viewed-heroes";

    private final StatefulRedisConnection<String, String> redis;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        redis = redisClient.connect();
    }

    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        LOGGER.info("Hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem)
                .thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> aBoolean && aLong > 0)
                .exceptionally(e -> {
                    handleErrors(e);
                    return false;
                });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        LOGGER.info("Incr hero in top");
        return redis
                .async()
                .zincrby(TOP_HEROES_KEY, 1, statItem.toJson().toString())
                .thenApply(d -> !d.isNaN())
                .exceptionally(e -> {
                    handleErrors(e);
                    return false;
                });
    }

    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        LOGGER.info("Add last visited hero");
        return redis
                .async()
                .zadd(VIEWED_HEROES_KEY, -new Timestamp(new Date().getTime()).getTime(), statItem.toJson().toString())
                .thenCombine(redis
                        .async()
                        .zremrangebyrank(VIEWED_HEROES_KEY, 5, -1), (ladd, lrem) -> ladd)
                .exceptionally(e -> {
                    handleErrors(e);
                    return -1L;
                });
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        LOGGER.info("Retrieved last viewed heroes");
        return redis
                .async()
                .zrange(VIEWED_HEROES_KEY, 0, count - 1)
                .thenApply(hs -> hs
                        .stream()
                        .map(StatItem::fromJson)
                        .collect(Collectors.toList()))
                .exceptionally(e -> {
                    handleErrors(e);
                    return Collections.singletonList(new StatItem("-1", "No result", "", ""));
                });
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        LOGGER.info("Retrieved top heroes");
        return redis
                .async()
                .zrevrangeWithScores(TOP_HEROES_KEY, 0, count - 1)
                .thenApply(hs -> hs
                        .stream()
                        .map(h -> new TopStatItem(StatItem.fromJson(h.getValue()), (long) h.getScore()))
                        .collect(Collectors.toList()))
                .exceptionally(e -> {
                    handleErrors(e);
                    final StatItem item = new StatItem("-1", "No result", "", "");
                    return Collections.singletonList(new TopStatItem(item, -1L));
                });
    }

    private void handleErrors(final Throwable e) {
        LOGGER.error("Error while contacting Redis: " + e.getMessage(), e);
    }
}
