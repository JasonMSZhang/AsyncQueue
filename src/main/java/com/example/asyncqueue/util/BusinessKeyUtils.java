package com.example.asyncqueue.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 幂等业务键生成：md5(subject + ":" + 排序后的实验ID列表)。
 *
 * <p>实验ID必须排序，否则 [test1,test2] 与 [test2,test1] 会被当成两个不同任务。</p>
 */
public final class BusinessKeyUtils {

    private BusinessKeyUtils() {
    }

    public static String generate(String subject, Collection<String> expIds) {
        List<String> sorted = new ArrayList<>(expIds);
        sorted.sort(String::compareTo);
        String raw = subject + ":" + String.join(",", sorted);
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = digest.digest(raw.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(32);
            for (byte b : bytes) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16))
                        .append(Character.forDigit(b & 0xf, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 算法不可用", e);
        }
    }
}
