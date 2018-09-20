package com.vbakh.s3.batch;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.vbakh.s3.utils.S3Utils;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import static java.util.stream.Collectors.toList;

/**
 * Updates S3 vector with {@link BatchLayer#UPDATE_INTERVAL} seconds interval
 */
public class BatchLayer {

    public static final String UPDATE_INTERVAL = "app.broadcast.update";

    public static void main(String[] args) {
        new BatchLayer().run();
    }

    public void run() {
        Config config = ConfigFactory.load();
        Random random = new Random();
        int interval = config.getInt(UPDATE_INTERVAL);

        Runnable updateVector = () -> S3Utils.updateVector(
                IntStream.range(0, 3).boxed().map(i -> random.nextInt(20)).collect(toList())
        );
        Executors.newScheduledThreadPool(1)
                .scheduleWithFixedDelay(updateVector, interval, interval, TimeUnit.SECONDS);
    }
}
