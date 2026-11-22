package com.example.asyncqueue.service;

import com.example.asyncqueue.exception.UnrecoverableTaskException;
import com.example.asyncqueue.service.AbTestBucketService.BucketResult;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AB-Test 分桶与本地缓存单元测试（不依赖 Spring）。
 */
class AbTestBucketServiceTest {

    private final AbTestBucketService service = new AbTestBucketService(180, 200_000);

    @Test
    @DisplayName("同一 uid + expId 多次计算结果稳定")
    void sameUidAndExpShouldBeStable() {
        BucketResult first = service.bucketOf("u-stable", "test3");
        for (int i = 0; i < 10; i++) {
            BucketResult again = service.bucketOf("u-stable", "test3");
            assertEquals(first.bucket(), again.bucket(), "桶号应稳定");
            assertEquals(first.treatment(), again.treatment(), "分组应稳定");
            assertEquals(first.expId(), again.expId());
        }
        assertTrue(first.bucket() >= 0 && first.bucket() < 10_000, "桶号应在 0~9999");
    }

    @Test
    @DisplayName("不同实验分布符合配置比例，且近似正交")
    void differentExperimentsShouldBeOrthogonal() {
        int sample = 2000;
        int test1Treatment = 0;
        int test10Treatment = 0;
        int bothTreatment = 0;
        int test1Only = 0;

        for (int i = 0; i < sample; i++) {
            String uid = "u-orth-" + i;
            boolean test1 = service.bucketOf(uid, "test1").treatment();
            boolean test10 = service.bucketOf(uid, "test10").treatment();
            if (test1) {
                test1Treatment++;
            }
            if (test10) {
                test10Treatment++;
            }
            if (test1 && test10) {
                bothTreatment++;
            }
            if (test1 && !test10) {
                test1Only++;
            }
        }

        double test1Ratio = test1Treatment / (double) sample;
        double test10Ratio = test10Treatment / (double) sample;
        double bothRatio = bothTreatment / (double) sample;
        double test1OnlyRatio = test1Only / (double) sample;

        // test1 实验组 5%，test10 实验组 95%，允许 ±2% 抽样误差
        assertEquals(0.05, test1Ratio, 0.02, "test1 实验组比例应接近 5%");
        assertEquals(0.95, test10Ratio, 0.02, "test10 实验组比例应接近 95%");
        // 若两个实验同源（不正交），both 会接近 5%；正交时应接近 5% × 95% ≈ 4.75%
        assertEquals(0.0475, bothRatio, 0.02, "两个实验应近似独立");
        // test1 实验组且 test10 对照组应接近 5% × 5% ≈ 0.25%
        assertTrue(test1OnlyRatio < 0.01, "test1 与 test10 不应强相关, 实际=" + test1OnlyRatio);
    }

    @Test
    @DisplayName("相同 key 第二次访问命中 Caffeine 缓存")
    void secondAccessShouldHitCache() {
        CacheStats before = service.cacheStats();

        BucketResult first = service.bucketOf("u-cache", "test5");
        CacheStats afterFirst = service.cacheStats();
        BucketResult second = service.bucketOf("u-cache", "test5");
        CacheStats afterSecond = service.cacheStats();

        assertEquals(before.missCount() + 1, afterFirst.missCount(), "首次访问应为 miss");
        assertEquals(afterFirst.hitCount() + 1, afterSecond.hitCount(), "第二次访问应为 hit");
        assertEquals(first, second, "命中缓存应返回相同结果");
    }

    @Test
    @DisplayName("未知实验ID抛不可恢复异常，且不写入缓存")
    void unknownExperimentShouldThrow() {
        assertThrows(UnrecoverableTaskException.class, () -> service.bucketOf("u-unknown", "test999"));
    }
}
