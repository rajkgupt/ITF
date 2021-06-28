package com.anz.itf.utils;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.io.File;

public class ParseOptions {
    public static final Options OPTIONS;

    static {
        OPTIONS = new Options();
        OPTIONS.addOption(new Option("schema", "schema", true, "Path to the schema file."));
        OPTIONS.addOption(new Option("data", "data", true, "Path to the data file."));
        OPTIONS.addOption(new Option("tag", "tag", true, "Path to the tag file."));
        OPTIONS.addOption(new Option("output", "output", true, "Path to the output file."));
    }

    public static void printHelp(Options options){

        System.out.println("| Short | Long | Description |");
        System.out.println("|-------|------|-------------|");

        for(Object optionO : options.getOptions()){
            Option option = (Option) optionO;
            System.out.print("| -");
            System.out.print(option.getOpt());
            System.out.print(" | --");
            System.out.print(option.getLongOpt());
            System.out.print(" | ");
            System.out.print(option.getDescription());
            System.out.println(" | ");
        }
    }

    public static HashMap parseOptions(Options options, String[] args) throws Exception{
        final CommandLine commandLine = new DefaultParser().parse(OPTIONS, args, false);

        HashMap<String, String> inputArgMap = new HashMap<>();

        String schema = commandLine.getOptionValue("schema");
        inputArgMap.put("schema", schema);

        String data = commandLine.getOptionValue("data");
        inputArgMap.put("data", data);

        String tag = commandLine.getOptionValue("tag");
        inputArgMap.put("tag", tag);

        String output = commandLine.getOptionValue("output");
        inputArgMap.put("output", output);

        return inputArgMap;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{

        Options options = ParseOptions.OPTIONS;

        if (args.length < 4) {
            ParseOptions.printHelp(options);
            System.exit(1);
        } else {
            HashMap<String,String> optionsMap = ParseOptions.parseOptions(options, args);
            optionsMap.entrySet().forEach(entry -> {
                System.out.println(entry.getKey() + " " + entry.getValue());
            });
        }
    }
}
