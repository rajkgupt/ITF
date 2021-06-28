package com.anz.itf.utils.json;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class JSONUtils {


    public static void main(String[] args) throws Exception {
        Gson gson = new Gson();

        try (Reader reader = new FileReader("staff.json")) {

            // Convert JSON File to Java Object
            Staff1 staff = gson.fromJson(reader, Staff1.class);

            // print staff
            System.out.println(staff);

            JsonReader reader1 = new JsonReader(new FileReader("staff.json"));
            Staff1 data1 = gson.fromJson(reader1, Staff1.class);
            //data1.toScreen(); // prints to screen some values


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
