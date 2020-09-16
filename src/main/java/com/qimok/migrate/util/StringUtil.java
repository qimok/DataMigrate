package com.qimok.migrate.util;

/**
 * String 转化工具类
 *
 * @author qimok
 * @since 2019-12-31
 */
@SuppressWarnings("checkstyle:magicNumber")
public class StringUtil {


    /**
     * 十六进制取模
     */
    public static Integer hexStrMod(String hexStr, Integer mod) {
        int ans = 0;
        for (int i = 0; i < hexStr.length(); ++i) {
            int t = convertHexCharToInt(hexStr.charAt(i));
            ans = (ans * 16 + t) % mod;
        }
        return ans;
    }

    /**
     * 将十六进制字符转换为十进制数字 f->15   a->10   8->8
     */
    private static int convertHexCharToInt(char c) {
        int t;
        if (c >= 'a' && c <= 'f') {
            t = c - 'a' + 10;
        } else if (c >= '0' && c <= '9') {
            t = c - '0';
        } else {
            t = 0;
        }
        return t;
    }

}
