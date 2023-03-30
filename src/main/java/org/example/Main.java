package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Map;

//class User extends SimpleFunction<String, String> {
//    @Override
//    public String apply(String input) {
//        String arr[] = input.split(",");
//        String Sname = arr[0];
//        String SGen = arr[1];
//        String output = "";
//        if (SGen.equals("1")) {
//            output = Sname + "," + "Male";
//        } else if (SGen.equals("2"))
//        {
//            output = Sname + "," + "Female";
//        }
//        return output;
//    }
//}
//class CustFilter extends DoFn<String ,String>{
//    @ProcessElement
//    public void processElement(ProcessContext c){
//        String line=c.element();
//        String arr[]=line.split(",");
//        if(arr[0].equals("user3")) {
//            c.output(line);
//        }
//        }
//    }

//class MyFilter implements SerializableFunction<String ,Boolean>{
//    @Override
//    public Boolean apply(String input)
//    {
//        return input.contains("user2");
//    }
//}

//PcollectionList<String> output = PcollectionList.of(output1).and(output2);
//PCollection<String> output3 = output.apply(Flatten.pCollections());
//class MyCityPartition implements Partition.PartitionFn<String > {
//    @Override
//    public int partitionFor(String elem, int numPartitions) {
//        String arr[] = elem.split(",");
//        if(arr[1].equals("user2"))
//        {
//            return 0;
//        }
//        else if(arr[1].equals("user1"))
//        {
//            return 1;
//        }
//        else
//        {
//            return 2;
//        }
//    }
//
//
//}

public class Main {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        PCollection<String> lines = pipeline.apply(TextIO.read().from("C:\\Users\\asus\\OneDrive\\Desktop\\Results\\input.txt"));
        //System.out.println(lines);
        //number of words in  the text file
//        PCollection<KV<String, Long>> wordCounts = lines.apply(FlatMapElements.into(TypeDescriptors.strings())
//                                        .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+")))).apply(Count.perElement());
//        PCollection<String> output1 = wordCounts.apply(MapElements.into(TypeDescriptors.strings())
//                .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()));

        PCollection<KV<String, Long>> output1 = lines
                .apply(
                        FlatMapElements.into(TypeDescriptors.strings())
                                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+")))
                )
                .apply(Count.<String>perElement());
        PCollection<String>s=output1.apply(MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()));
        s.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        //output1.apply(TextIO.write().to("C:\\Users\\asus\\OneDrive\\Desktop\\Results\\output1.txt").withNumShards(1).withSuffix(".txt"));
        pipeline.run();
    }
}
