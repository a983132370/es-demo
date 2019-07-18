package com.example.esdemo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class EsDemoApplicationTests {

    @Test
    public void contextLoads() {
        long allRecord = 2L;
        long pageSize = 2L;

        System.out.println(allRecord/pageSize);
        System.out.println(allRecord%pageSize);
        System.out.println(allRecord/pageSize + (allRecord%pageSize > 0 ? 1 : 0));

    }

}
