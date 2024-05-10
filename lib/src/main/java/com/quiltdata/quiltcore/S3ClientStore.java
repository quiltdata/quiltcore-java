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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a store for S3 clients and provides methods to retrieve S3 clients based on bucket names.
 */
public class S3ClientStore {
    
    private static final Logger logger = LoggerFactory.getLogger(S3ClientStore.class);
    private static final S3Client LOCATION_CLIENT = createClient(Region.US_EAST_1);

    private static Map<String, Region> regionMap = Collections.synchronizedMap(new HashMap<>());
    private static Map<Region, S3AsyncClient> asyncClientMap = Collections.synchronizedMap(new HashMap<>());
    private static Map<Region, S3Client> clientMap = Collections.synchronizedMap(new HashMap<>());

    /**
     * Retrieves an asynchronous S3 client for the specified bucket.
     *
     * @param bucket The name of the bucket.
     * @return An asynchronous S3 client.
     */
    public static S3AsyncClient getAsyncClient(String bucket) {
        logger.debug("Creating S3 client for bucket: {}", bucket);
        Region region = getBucketRegion(bucket);
        return asyncClientMap.computeIfAbsent(region, S3ClientStore::createAsyncClient);
    }

    /**
     * Retrieves a synchronous S3 client for the specified bucket.
     *
     * @param bucket The name of the bucket.
     * @return A synchronous S3 client.
     */
    public static S3Client getClient(String bucket) {
        Region region = getBucketRegion(bucket);
        return clientMap.computeIfAbsent(region, S3ClientStore::createClient);
    }

    /**
     * Retrieves the region of the specified bucket.
     *
     * @param bucket The name of the bucket.
     * @return The region of the bucket.
     */
    public static Region getBucketRegion(String bucket) {
        return regionMap.computeIfAbsent(bucket, S3ClientStore::findBucketRegion);
    }

    private static Region findBucketRegion(String bucket) {
        SdkHttpResponse response;
        logger.debug("Finding region for bucket: {}", bucket);
        try {
            response = LOCATION_CLIENT.headBucket(builder -> builder.bucket(bucket)).sdkHttpResponse();
        } catch (S3Exception e) {
            if (e.statusCode() == 301 || e.statusCode() == 400) {
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

    private static S3AsyncClient createAsyncClient(Region region) {
        return S3AsyncClient.crtBuilder()
            .region(region)
            .minimumPartSizeInBytes(8L * 1024 * 1024)
            .build();
    }

    private static S3Client createClient(Region region) {
        return S3Client.builder().region(region).build();
    }
}
