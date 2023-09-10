package com.cch.spark.rdd;

import java.util.ArrayList;
import java.util.List;

public class test {

    public static void main(String[] args) {

        AAA bbb = new BBB();
        test(bbb);
    }

    public static void test(AAA aaa){
        System.out.println("aaa");
    }

    public static void test(BBB bbb){
        System.out.println("bbb");
    }
}

class AAA{

}

class BBB extends AAA{

}
