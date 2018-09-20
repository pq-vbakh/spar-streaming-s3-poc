package com.vbakh.s3.utils;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.*;
import java.util.List;

public class S3Utils {

    private static final String S3_BUCKET = "app.s3.bucket";
    private static final String S3_KEY = "app.s3.key";
    private static final Config config = ConfigFactory.load();

    public static void updateVector(List<Integer> list) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        InputStream stream = new ByteArrayInputStream(CollectionUtils.toByteArray(list));
        s3Client.putObject(
                new PutObjectRequest(
                        config.getString(S3_BUCKET), config.getString(S3_KEY), stream, new ObjectMetadata()
                )
        );
        System.out.println("Updated vector to " + list);
    }

    public static List<Integer> getVector() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        InputStream objectStream = s3Client.getObject(
                new GetObjectRequest(config.getString(S3_BUCKET), config.getString(S3_KEY))
        ).getObjectContent();
        List<Integer> list = CollectionUtils.fromInputStream(objectStream);
        System.out.println("Refreshed vector to " + list);
        return list;
    }
}
