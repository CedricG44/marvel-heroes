package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import models.StatItem;
import models.TopStatItem;
import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> redis;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
        redis = this.redisClient.connect();
    }

    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("Hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem)
                .thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> aBoolean && aLong > 0);
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        logger.info("Incr hero in top");
        return redis
                .async()
                .zincrby("top-heroes", 1, statItem.toJson().toString())
                .thenApply(d -> !d.isNaN());
    }

    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        logger.info("Add last visited hero");
        return redis
                .async()
                .zadd("viewed-heroes", -new Timestamp(new Date().getTime()).getTime(), statItem.toJson().toString())
                .thenCombine(redis
                        .async()
                        .zremrangebyrank("viewed-heroes", 5, -1), (ladd, lrem) -> ladd);
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last viewed heroes");
        return redis
                .async()
                .zrange("viewed-heroes", 0, count - 1)
                .thenApply(hs -> hs
                        .stream()
                        .map(StatItem::fromJson)
                        .collect(Collectors.toList()));
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved top heroes");
        return redis
                .async()
                .zrevrangeWithScores("top-heroes", 0, count - 1)
                .thenApply(hs -> hs
                        .stream()
                        .map(h -> new TopStatItem(StatItem.fromJson(h.getValue()), (long) h.getScore()))
                        .collect(Collectors.toList()));
    }
}
