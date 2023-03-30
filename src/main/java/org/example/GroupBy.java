package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import java.io.Serializable;

class StringtoKV extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        String arr[] = line.split(",");
        c.output(KV.of(arr[0], Integer.valueOf(arr[1])));
    }

}

    public class GroupBy {

        public static void main(String[] args) {
            Pipeline pipeline = Pipeline.create();
            PCollection<String> input = pipeline.apply(TextIO.read().from("C:\\Users\\asus\\OneDrive\\Desktop\\Results\\input.csv"));
            // convert string --> KV
            PCollection<KV<String, Integer>> output = input.apply(ParDo.of(new StringtoKV()));
            //key vale , iterable
            PCollection<KV<String, Iterable<Integer>>> output1 = output.apply(GroupByKey.create());
            //convert KV --> string
            PCollection<String> output2 = output1.apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String key = c.element().getKey();
                    Iterable<Integer> values = c.element().getValue();
                    Integer sum = 0;
                    for (Integer value : values) {
                        sum += value;
                    }
                    c.output(key + "," + (sum.toString()));
                }

            }

            ));




            output2.apply(TextIO.write().to("C:\\Users\\asus\\OneDrive\\Desktop\\Results\\Groupby.csv").withNumShards(1).withSuffix(".csv"));
            pipeline.run();

        }
    }
