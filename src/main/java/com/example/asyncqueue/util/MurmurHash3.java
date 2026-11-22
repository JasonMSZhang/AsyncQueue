package com.example.asyncqueue.util;

/**
 * MurmurHash3 x86 32 位实现（seed 默认为 0）。
 *
 * <p>AB-Test 分桶必须锁定唯一实现：算法、seed、字节序（UTF-8 + 小端读取）三者任一变化都会导致
 * 同一用户在不同端落到不同桶。本类只提供 32 位版本，避免 murmur3_128 取高/低半区造成的歧义。</p>
 */
public final class MurmurHash3 {

    private MurmurHash3() {
    }

    /**
     * 计算 32 位 MurmurHash3。
     *
     * @param data 原始字符串，内部按 UTF-8 编码
     * @param seed 种子，分桶场景固定传 0
     * @return 32 位哈希值，可能为负数，取桶时必须使用 {@link Math#floorMod(int, int)}
     */
    public static int hash32(String data, int seed) {
        byte[] bytes = data.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int length = bytes.length;
        int h1 = seed;
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        // 按 4 字节一组处理（小端）
        int roundedEnd = length & 0xfffffffc;
        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (bytes[i] & 0xff)
                    | ((bytes[i + 1] & 0xff) << 8)
                    | ((bytes[i + 2] & 0xff) << 16)
                    | ((bytes[i + 3] & 0xff) << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // 处理尾部剩余 1~3 字节
        int k1 = 0;
        switch (length & 0x03) {
            case 3:
                k1 ^= (bytes[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k1 ^= (bytes[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k1 ^= (bytes[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                break;
            default:
                break;
        }

        // 最终混淆
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
