package com.vbakh.s3.speed;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStream$;
import org.apache.spark.streaming.dstream.DStream;
import java.util.List;
import static java.util.stream.Collectors.toList;

/**
 * Reads stream from localhost:9999 socket, multiply each micro-batch by S3 broadcasted vector
 */
public class SpeedLayer {

    public static final String HOST = "app.stream.input.host";
    public static final String PORT = "app.stream.input.port";
    private final SparkConf conf;
    private final StreamingContext ssc;

    public static void main(String[] args) {
        new SpeedLayer().run();
    }

    public SpeedLayer() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        conf = new SparkConf().setMaster("local[*]").setAppName("Test App");
        ssc = new StreamingContext(conf, Seconds.apply(10));
    }

    private void run()  {
        JavaDStream<List<Integer>> output = fromSocket().transform(rdd -> {
            List<Integer> broadcast = BroadcastWrapper.getInstance()
                    .updateAndGet(ssc.sparkContext())
                    .getValue();
            // multiply vector by each rdd element
            return rdd.map(value -> broadcast.stream().map(i -> i * value).collect(toList()));
        });

        output.print();

        ssc.start();
        ssc.awaitTermination();
    }

    /**
     * Creates JavaDStream from localhost:9999 socket. You can write to socket from command line with 'nc -lk 9999'
     * @return
     */
    private JavaDStream<Integer> fromSocket() {
        Config config = ConfigFactory.load();
        DStream<String> input = ssc.socketTextStream(config.getString(HOST), config.getInt(PORT), StorageLevel.MEMORY_AND_DISK_2());
        JavaDStream<String> inputStream = JavaDStream$.MODULE$.fromDStream(input, scala.reflect.ClassTag$.MODULE$.apply(String.class));
        return inputStream.map(Integer::valueOf);
    }
}
