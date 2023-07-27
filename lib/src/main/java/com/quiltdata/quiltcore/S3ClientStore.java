
package com.quiltdata.quiltcore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3ClientStore {
    private static final S3Client LOCATION_CLIENT = S3Client.builder().region(Region.US_EAST_1).build();

    private static Map<String, S3AsyncClient> clientMap = Collections.synchronizedMap(new HashMap<>());

    public static S3AsyncClient getClient(String bucket) {
        return clientMap.computeIfAbsent(bucket, S3ClientStore::createClient);
    }

    public static Region getBucketRegion(String bucket) {
        SdkHttpResponse response;
        try {
            response = LOCATION_CLIENT.headBucket(builder -> builder.bucket(bucket)).sdkHttpResponse();
        } catch (S3Exception e) {
            if (e.statusCode() == 301) {
                response = e.awsErrorDetails().sdkHttpResponse();
            } else {
                throw e;
            }
        }
        String regionStr = response
            .firstMatchingHeader("x-amz-bucket-region")
            .orElseThrow(() -> new NoSuchElementException("Couldn't find the 'x-amz-bucket-region' header"));

        return Region.of(regionStr);
    }

    private static S3AsyncClient createClient(String bucket) {
        Region region = getBucketRegion(bucket);
        return S3AsyncClient.crtBuilder().region(region).build();
    }
}
