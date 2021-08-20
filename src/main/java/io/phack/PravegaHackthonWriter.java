package io.phack;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PravegaHackthonWriter {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(PravegaHackthonWriter.class);

    // Application parameters
    //   host - host running netcat, default 127.0.0.1
    //   port - port on which netcat listens, default 9999
    //   stream - the Pravega stream to write data to, default examples/wordcount
    //   controller - the Pravega controller uri, default tcp://127.0.0.1:9090

    public static void main(String[] args) throws Exception {
        LOG.info("Starting WordCountWriter...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // retrieve the socket host and port information to read the incoming data from
        String host = params.get(Constants.HOST_PARAM, Constants.DEFAULT_HOST);
        int port = Integer.parseInt(params.get(Constants.PORT_PARAM, Constants.DEFAULT_PORT));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> dataStream = env.socketTextStream(host, port);

        // create the Pravega sink to write a stream of text
        FlinkPravegaWriter<String> writer = FlinkPravegaWriter.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withEventRouter(new EventRouter())
                .withSerializationSchema(new SimpleStringSchema())
                .build();
        dataStream.addSink(writer).name("Pravega Stream");

        // create another output sink to print to stdout for verification
        dataStream.print().name("stdout");

        // execute within the Flink environment
        env.execute("WordCountWriter");

        LOG.info("Ending WordCountWriter...");
    }

    /*
     * Event Router class
     */
    public static class EventRouter implements PravegaEventRouter<String> {
        // Ordering - events with the same routing key will always be
        // read in the order they were written
        @Override
        public String getRoutingKey(String event) {
            return "SameRoutingKey";
        }
    }
}
