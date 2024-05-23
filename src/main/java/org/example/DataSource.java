package org.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

public class DataSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        long startTime = System.currentTimeMillis();
        try (BufferedReader reader = new BufferedReader(new FileReader("data/input_data_300000.csv"))) {
            String line = reader.readLine();
            while (line != null) {

                sourceContext.collect(line);

                line = reader.readLine();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("===== Period Duration: " + (endTime - startTime) + "ms ======");
            System.out.println("finished");
        }
    }

    @Override
    public void cancel() {

    }
}
