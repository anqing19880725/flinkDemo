package com.atguigu.wc;

import java.util.Scanner;

public class test {
    public static void main(String[] args) {


//        String[] countryArray = {"aa","bb","cc","dd","ee"};
//        System.out.println(countryArray[0]);
//        System.out.println(countryArray[1]);
//        System.out.println(countryArray[2]);
//        System.out.println(countryArray[3]);


        System.out.println("输入一个字符串：");
        String str = null;
        Scanner cin = new Scanner(System.in);
        while (cin.hasNext()) {
            str = cin.nextLine();
            break;
        }
        String newStr1 = "";
        String newStr2 = "";
        for (int i = 0; i < str.length(); i++) {

            if (str.substring(i, i + 1).matches("^[A-Z]+$")) {
                newStr2 = str.substring(i, i + 1).toLowerCase();
            } else if (str.substring(i, i + 1).matches("^[a-z]+$")) {
                newStr2 = str.substring(i, i + 1).toUpperCase();
            } else {
                newStr2 = str.substring(i, i + 1);
            }
            newStr1 = newStr1 + newStr2;
        }
        System.out.println("输出结果：");
        System.out.println(newStr1.length());
    }
}






