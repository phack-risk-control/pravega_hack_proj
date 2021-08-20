package io.phack;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaHackthonReader {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(PravegaHackthonReader.class);

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.

    // Application parameters
    //   stream - default examples/wordcount
    //   controller - default tcp://127.0.0.1:9090

    public static void main(String[] args) throws Exception {
        LOG.info("Starting WordCountReader...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the Pravega source to read a stream of text
        FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(new SimpleStringSchema())
                .build();

        // count each word over a 10 second time period
        DataStream<WordCount> dataStream = env.addSource(source).name("Pravega Stream")
                .flatMap(new PravegaHackthonReader.Splitter())
                .keyBy("word")
                .timeWindow(Time.seconds(10))
                .sum("count");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Neo4jOutputFormat<Tuple2<String, Integer>> outputFormat = Neo4jOutputFormat
                .buildNeo4jOutputFormat()
                .setRestURI("http://localhost:7475/db/data/")
                .setConnectTimeout(1_000)
                .setReadTimeout(1_000)
                .setCypherQuery("UNWIND {inserts} AS i CREATE (a:User {name:i.name, born:i.born})")
                .addParameterKey(0, "name")
                .addParameterKey(1, "born")
                .setTaskBatchSize(1000)
                .finish();

        env.fromElements(new Tuple2<>("Alice", 1984),new Tuple2<>("Bob", 1976)).output(outputFormat);

        env.execute();

        // create an output sink to print to stdout for verification
        dataStream.print();

        // execute within the Flink environment
        env.execute("WordCountReader");

        LOG.info("Ending WordCountReader...");
    }

    // split data into word by space
    private static class Splitter implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String line, Collector<WordCount> out) throws Exception {
            for (String word: line.split(Constants.WORD_SEPARATOR)) {
                out.collect(new WordCount(word, 1));
            }
        }
    }

}
