package com.alexwang.flink.chapter04.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.Socket;

public class CustomSinkNewAPIExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(20_000, CheckpointingMode.AT_LEAST_ONCE);
        GeneratorFunction<Long, String> function = (Long num) -> String.format("The generated text line and Num:%s", num);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "source")
                .sinkTo(CustomSink.of("localhost", 3456));
        env.execute();
    }

    private static class CustomSink implements Sink<String> {

        private final String hostname;
        private final int port;

        private CustomSink(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        public static CustomSink of(String hostname, int port) {
            return new CustomSink(hostname, port);
        }

        @Override
        public SinkWriter<String> createWriter(InitContext context) throws IOException {
            CustomSinkWriter customSinkWriter = new CustomSinkWriter(hostname, port);
            customSinkWriter.open();
            return customSinkWriter;
        }
    }

    private static class CustomSinkWriter implements SinkWriter<String>, Serializable {

        private final String hostname;
        private final int port;
        private Socket socket;
        private PrintStream stream;

        private CustomSinkWriter(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        void open() throws IOException {
            this.socket = new Socket(hostname, port);
            this.stream = new PrintStream(socket.getOutputStream());
        }

        @Override
        public void write(String element, SinkWriter.Context context) throws IOException, InterruptedException {
            this.stream.println(element);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            this.stream.flush();
        }

        @Override
        public void close() throws Exception {
            if (this.stream != null)
                this.stream.close();
            if (this.socket != null && !this.socket.isClosed())
                this.socket.close();
        }
    }
}
