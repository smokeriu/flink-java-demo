package org.example.other;

import org.junit.jupiter.api.Test;

public class IpTest {
    @Test
    void ipConvert() {
        long longIp = -986913820;
        final String s = Long.toBinaryString(longIp);
        final long l = Long.parseUnsignedLong(s, 2);
        System.out.println(l);

        // 直接右移24位
        final String last = String.valueOf((longIp >>> 24));
        final String secondLast = String.valueOf((longIp & 0x00FFFFFF) >>> 16);
        final String secondFirst = String.valueOf((longIp & 0x0000FFFF) >>> 8);
        final String first = String.valueOf((longIp & 0x000000FF));

        System.out.printf("%s.%s.%s.%s", first, secondFirst, secondLast, last);
    }

    public Integer Biannary2Decimal(long bi) {
        String binStr = bi + "";
        Integer sum = 0;
        int len = binStr.length();
        for (int i = 1; i <= len; i++) {
            //第i位 的数字为：
            int dt = Integer.parseInt(binStr.substring(i - 1, i));
            sum += (int) Math.pow(2, len - i) * dt;
        }
        return sum;
    }
}
