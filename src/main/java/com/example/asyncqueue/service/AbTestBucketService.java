package com.example.asyncqueue.service;

import com.example.asyncqueue.exception.UnrecoverableTaskException;
import com.example.asyncqueue.util.MurmurHash3;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;

/**
 * AB-Test 分桶服务。
 *
 * <p>分桶规则：hash 原始串为 {@code uid:expId}，使用 MurmurHash3（32 位、seed=0），
 * 对 10000 取模得到桶号（0~9999）；桶号小于实验组桶上限即为实验组，否则为对照组。</p>
 *
 * <p>本地缓存：Caffeine，key 为 {@code uid:expId}，TTL 180 秒（expireAfterWrite），
 * 命中直接返回；未命中则计算并写回。使用 {@code cache.get(key, mapping)} 保证同一 key 只计算一次，
 * 避免缓存击穿。</p>
 */
@Slf4j
@Service
public class AbTestBucketService {

    /** 分桶总数量 */
    public static final int TOTAL_BUCKETS = 10_000;

    /** 哈希种子，固定为 0 */
    private static final int HASH_SEED = 0;

    /**
     * 实验组桶上限：bucket < limit 为实验组，其余为对照组。
     * test1=5% -> 0~499；test2=15% -> 0~1499；…… test10=95% -> 0~9499。
     */
    private static final Map<String, Integer> TREATMENT_BUCKET_LIMITS = Map.ofEntries(
            Map.entry("test1", 500),
            Map.entry("test2", 1500),
            Map.entry("test3", 2500),
            Map.entry("test4", 3500),
            Map.entry("test5", 4500),
            Map.entry("test6", 5500),
            Map.entry("test7", 6500),
            Map.entry("test8", 7500),
            Map.entry("test9", 8500),
            Map.entry("test10", 9500)
    );

    private final Cache<String, BucketResult> cache;

    public AbTestBucketService(@Value("${async-queue.cache.ttl-seconds:180}") long ttlSeconds,
                               @Value("${async-queue.cache.max-size:200000}") long maxSize) {
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .maximumSize(maxSize)
                .recordStats()
                .build();
        log.info("AB 分桶缓存初始化: ttl={}s, maxSize={}", ttlSeconds, maxSize);
    }

    /**
     * 计算单个用户在某实验下的分桶结果（带本地缓存）。
     *
     * @param uid   用户标识（uid 或 deviceId）
     * @param expId 实验ID
     * @return 分桶结果
     */
    public BucketResult bucketOf(String uid, String expId) {
        String cacheKey = uid + ":" + expId;
        // 命中直接返回；未命中执行 compute 并写入缓存，同一 key 并发只会计算一次
        return cache.get(cacheKey, key -> compute(uid, expId));
    }

    /**
     * 是否支持该实验ID，供接口层提前校验，避免无效任务进入死信。
     */
    public boolean supports(String expId) {
        return expId != null && TREATMENT_BUCKET_LIMITS.containsKey(expId);
    }

    /**
     * 缓存统计（命中率、加载次数等），供监控与测试使用。
     */
    public CacheStats cacheStats() {
        return cache.stats();
    }

    private BucketResult compute(String uid, String expId) {
        Integer limit = TREATMENT_BUCKET_LIMITS.get(expId);
        if (limit == null) {
            // 未知实验属于不可恢复错误，重试不会变好
            throw new UnrecoverableTaskException("未知实验ID: " + expId);
        }
        int hash = MurmurHash3.hash32(uid + ":" + expId, HASH_SEED);
        // floorMod 保证负 hash 也落在 0~9999，不能用 %
        int bucket = Math.floorMod(hash, TOTAL_BUCKETS);
        return new BucketResult(expId, bucket, bucket < limit);
    }

    /**
     * 单个实验的分桶结果。
     *
     * @param expId     实验ID
     * @param bucket    桶号 0~9999
     * @param treatment true=实验组，false=对照组
     */
    public record BucketResult(String expId, int bucket, boolean treatment) {
    }
}
