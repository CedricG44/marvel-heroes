package repository;

import io.lettuce.core.RedisClient;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("Hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem)
                .thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> aBoolean && aLong > 0);
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        logger.info("Incr hero in top");
        return redisClient
                .connect()
                .async()
                .zincrby("top-heroes", 1, statItem.toJson().toString())
                .thenApply(d -> !d.isNaN());
    }

    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        // TODO
        return CompletableFuture.completedFuture(1L);
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        // TODO
        List<StatItem> lastsHeroes = Arrays.asList(StatItemSamples.IronMan(), StatItemSamples.Thor(), StatItemSamples.CaptainAmerica(), StatItemSamples.BlackWidow(), StatItemSamples.MsMarvel());
        return CompletableFuture.completedFuture(lastsHeroes);
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved top heroes");
        return redisClient
                .connect()
                .async()
                .zrevrangeWithScores("top-heroes", 0, count)
                .thenApply(hs -> hs
                        .stream()
                        .map(h -> new TopStatItem(StatItem.fromJson(h.getValue()), (long) h.getScore()))
                        .collect(Collectors.toList()));
    }
}
