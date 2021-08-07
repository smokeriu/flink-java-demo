package org.example.other;

import org.junit.jupiter.api.Test;

public class OtherTest {

    @Test
    public void test1(){
        String s = "aaabb";
        final String[] bs = s.split("b");
        System.out.println(bs.length);
        for (String b : bs) {
            System.out.println(b);
        }
    }
}
