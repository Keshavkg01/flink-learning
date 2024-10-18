package com.alexwang.flink.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountBatchProcess {
    public static void main(String[] args) throws Exception {
        //1. create the execution env.
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. read data from batch files
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        //3. process and transformation data

        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector)
                    throws Exception {
                Arrays.stream(line.split("\\s+")).forEach(
                        word -> collector.collect(Tuple2.of(word, 1L))
                );
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> unsortedGrouping = flatMapOperator.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> aggregateOperator = unsortedGrouping.sum(1);

        aggregateOperator.print();
    }
}
