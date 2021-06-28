package com.anz.itf.utils.json;



    // Java program to read JSON from a file

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.Iterator;
import java.util.Map;

public class SparkSchemaJasonParser
{
    public static void main(String[] args) throws Exception
    {
        // parsing file "JSONExample.json"
        Object obj = new JSONParser().parse(new FileReader("SparkSchema.json"));

        // typecasting obj to JSONObject
        JSONObject jo = (JSONObject) obj;



        // getting columns
        JSONArray ja = (JSONArray) jo.get("columns");

        // iterating phoneNumbers
        Iterator itr2 = ja.iterator();

        while (itr2.hasNext())
        {
            Iterator<Map.Entry> itr1 =  ((Map) itr2.next()).entrySet().iterator();
            while (itr1.hasNext()) {
                Map.Entry pair = itr1.next();
                System.out.println(pair.getKey() + " : " + pair.getValue());
            }
        }


        // getting primary_keys
        JSONArray primary_keys = (JSONArray) jo.get("primary_keys");

        // iterating phoneNumbers
        Iterator itr3 = primary_keys.iterator();

        while (itr3.hasNext())
        {
            String key1 =  (String) itr3.next();

            System.out.println(key1);

        }


    }
}

