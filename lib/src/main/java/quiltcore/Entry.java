package quiltcore;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;


public class Entry {
    public static enum HashType {
        SHA256,
    }

    public static class Hash {
        public HashType type;
        public String value;
    }

    public String physicalKey;
    public long size;
    public Hash hash;
    public Map<String, ?> meta;

    public byte[] get() throws URISyntaxException {
        URI uri = new URI(physicalKey);
        if (uri.getScheme().equals("s3")) {
            String bucket = uri.getHost();
            String key = uri.getPath().substring(1);  // Remove /
            String versionId = null;
            for (NameValuePair p : URLEncodedUtils.parse(uri.getQuery(), StandardCharsets.UTF_8)) {
                if (p.getName().equals("versionId")) {
                    versionId = p.getValue();
                }
            }

            ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
            // Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder()
                // .region(region)
                .credentialsProvider(credentialsProvider)
                .build();
            GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .bucket(bucket)
                .key(key)
                .versionId(versionId)
                .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            return data;
        } else {
            throw new RuntimeException("Not implemented");
        }
    }
}
