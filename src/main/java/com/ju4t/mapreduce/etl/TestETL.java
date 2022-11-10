package com.ju4t.mapreduce.etl;

public class TestETL {
    public static void main(String[] args) {
        String check = "^1(3\\d|4[5-9]|5[0-35-9]|6[567]|7[0-8]|8\\d|9[0-35-9])\\d{8}$";
        String phone = "18999177828";

        System.out.println(phone.matches(check));
    }
}
