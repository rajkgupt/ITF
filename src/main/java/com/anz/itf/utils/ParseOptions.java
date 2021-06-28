package com.anz.itf.utils;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.*;
import java.util.HashMap;


public class ParseOptions {
    public static final Options OPTIONS;

    static {

        OPTIONS = new Options();

        OptionBuilder.withArgName("path");
        OptionBuilder.hasArgs();
        OptionBuilder.withDescription("Path to the schema file");
        OptionBuilder.withLongOpt("schema");
        OptionBuilder.isRequired();
        OPTIONS.addOption(OptionBuilder.create("s"));

        OptionBuilder.withArgName("path");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Path to the data file");
        OptionBuilder.withLongOpt("data");
        OptionBuilder.isRequired();
        OPTIONS.addOption(OptionBuilder.create("d"));

        OptionBuilder.withArgName("path");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Path to the tag file");
        OptionBuilder.withLongOpt("tag");
        OPTIONS.addOption(OptionBuilder.create("t"));

        OptionBuilder.withArgName("path");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("Path to the output file");
        OptionBuilder.withLongOpt("output");
        OPTIONS.addOption(OptionBuilder.create("o"));
    }

    public static void printHelp(Options options){
        //Options options = ;

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
        final CommandLine commandLine = new PosixParser().parse(OPTIONS, args, false);

        HashMap<String, String> inputArgMap = new HashMap<>();


        Option opt = options.getOption("schema");
        String val = opt.getLongOpt();
        //System.out.print("Value:" + val);

        String schema = commandLine.getOptionValue("schema");
        //System.out.print("schema:" + schema);
        inputArgMap.put("schema", schema);

        String data = commandLine.getOptionValue("data");
        //System.out.print("data:" + data);
        inputArgMap.put("data", data);

        String tag = commandLine.getOptionValue("tag");
        //System.out.print("tag:" + tag);
        inputArgMap.put("tag", tag);

        String output = commandLine.getOptionValue("output");
        //System.out.print("output:" + output);
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
            ParseOptions.parseOptions(options, args);
        }





    }

}
