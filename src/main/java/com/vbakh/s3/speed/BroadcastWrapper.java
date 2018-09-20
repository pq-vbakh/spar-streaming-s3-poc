package com.vbakh.s3.speed;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.vbakh.s3.utils.S3Utils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Responsible for updating broadcast variable with {@link BroadcastWrapper#REFRESH_INTERVAL} seconds interval
 */
public class BroadcastWrapper {

    public static final String REFRESH_INTERVAL = "app.broadcast.refresh";
    private static final BroadcastWrapper obj = new BroadcastWrapper();
    private final Config config = ConfigFactory.load();
    private Broadcast<List<Integer>> broadcastVar;
    private Date lastUpdatedAt = Calendar.getInstance().getTime();

    private BroadcastWrapper(){
    }

    public static BroadcastWrapper getInstance() {
        return obj;
    }

    public JavaSparkContext getSparkContext(SparkContext sc) {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        return jsc;
    }

    public Broadcast<List<Integer>> updateAndGet(SparkContext sparkContext){
        Date currentDate = Calendar.getInstance().getTime();
        long diff = currentDate.getTime() - lastUpdatedAt.getTime();
        if (broadcastVar == null || diff > config.getInt(REFRESH_INTERVAL) * 1000) {
            if (broadcastVar != null)
                broadcastVar.unpersist();
            lastUpdatedAt = new Date(System.currentTimeMillis());

            List<Integer> data = S3Utils.getVector();

            broadcastVar = getSparkContext(sparkContext).broadcast(data);
        }
        return broadcastVar;
    }

}
