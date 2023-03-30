package practice;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class Merge_PCollection {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        final String[] arr = {"water Ocean", "fire Sun", "air Sky", "earth Land", "Ether Space"};
        final List<String> elements = List.of(arr);
        //elements.stream().forEach(System.out::println);
        Pipeline p = Pipeline.create();
        PCollection<String> p1 = p.apply("Create1", Create.of(elements));
        PCollection<String> p2 = p1.apply("Create Words", FlatMapElements.into(TypeDescriptors.strings())
                .via((String c) -> Arrays.asList(c.split(" "))));
        PCollection<KV<String,Long>> p3=p2.apply(Count.<String>perElement());
        PCollection<String> p4=p3.apply(MapElements.into(TypeDescriptors.strings())
                .via((KV<String,Long> c) -> c.getKey()+" : "+c.getValue().toString()));
        p4.apply("print", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));
        p2.apply("print", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        p.run();

    }
}
