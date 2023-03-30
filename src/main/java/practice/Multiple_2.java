package practice;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TypeDescriptors;
public class Multiple_2 {

        public static void main(String[] args) {
            // TODO Auto-generated method stub
            Pipeline pipeline = Pipeline.create();


            PCollection<Integer> p1 = pipeline.apply("Create1", Create.of(1, 2, 3, 4, 5));

// Create a new PCollection by multiplying each element in p1 by 2
            PCollection<Integer> p2 = p1.apply("MultiplyBy2", MapElements.into(TypeDescriptors.integers())
                    .via((Integer element) -> element * 2))
                    .apply(Sum.integersGlobally());
            p2.apply(ParDo.of(new DoFn<Integer, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    System.out.println(c.element());
                }
            }));


// Print the elements of p2
//
// This will print the following to the console:
// 2
// 4


// Run the pipeline
            pipeline.run();

        }
}
