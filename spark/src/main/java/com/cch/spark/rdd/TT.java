package com.cch.spark.rdd;

import java.util.Arrays;
import java.util.List;

public class TT {

    public static void main(String[] args) {

        String [] ls = { "a","b"};
        String[] ls2 = {ls[0]};
        String [] ls1 = {"a"};

        System.out.println(Arrays.toString(ls2));
        System.out.println(ls1.toString());
        System.out.println(ls2 == ls1);
        System.out.println(ls2.equals(ls1));
        System.out.println(Arrays.equals(ls2, ls1));

    }
}
