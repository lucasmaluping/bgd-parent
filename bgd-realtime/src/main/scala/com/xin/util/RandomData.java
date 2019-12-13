package com.xin.util;

import java.util.Random;

public class RandomData {
    public static void main(String[] args) throws InterruptedException {
        String[] strs ={"aa","bb","cc","dd","ee","ff"};
        Random random = new Random();

        while (true){
            int i = random.nextInt(strs.length);
            String str = strs[i];
            System.out.println(str);
            Thread.sleep(100);
            ProducerUtil.send(str);
        }
    }
}
