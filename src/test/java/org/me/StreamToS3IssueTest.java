package org.me;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Expect content in all tests successfully uploaded and has the same bytes.
 */
public class StreamToS3IssueTest {

    private static final LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
    // Test container with mock s3 implementation.
    private static final GenericContainer<?> container = new GenericContainer<>("adobe/s3mock:2.1.34")
        .withExposedPorts(9090)
        .waitingFor(new LogMessageWaitStrategy().withRegEx("^.*Started S3MockApplication in.*"));

    static {
        ctx.getLogger("ROOT").setLevel(Level.INFO);
    }

    private final S3Client s3Client = getClient();

    private StreamToS3Issue issue;

    @BeforeAll
    static void beforeAll() {
        container.start();
    }

    @AfterAll
    static void afterAll() {
        container.stop();
    }

    @BeforeEach
    void setUp() {
        s3Client.createBucket(b -> b.bucket("test-bucket"));
        ctx.getLogger("software.amazon.awssdk").setLevel(Level.TRACE);
    }

    @AfterEach
    void tearDown() {
        ctx.getLogger("software.amazon.awssdk").setLevel(Level.INFO);
        if (nonNull(issue)) {
            issue.stop();
        }
        // clear all created files and buckets
        var buckets = s3Client.listBuckets().buckets();
        for (var bucket : buckets) {
            var objects = s3Client.listObjects(b -> b.bucket(bucket.name())).contents();
            for (var object : objects) {
                s3Client.deleteObject(b -> b
                    .bucket(bucket.name())
                    .key(object.key()));
            }
            s3Client.deleteBucket(b -> b.bucket(bucket.name()));
        }
    }

    @Test
    public void streamWithChecksumValidation1Mb() {
        startIssue(true);
        var bytes = generateBytes(1_000_000);

        var response = sendFile(bytes);
        assertEquals(HttpResponseStatus.OK, response.status());

        var s3Bytes = s3Client.getObjectAsBytes(b -> b
            .bucket("test-bucket")
            .key(Integer.toString(issue.sequence.get() - 1))
        ).asByteArray();
        assertEquals(bytes.length, s3Bytes.length);
        assertArrayEquals(bytes, s3Bytes);
    }

    @Test
    public void streamWithoutChecksumValidation1Mb() {
        startIssue(false);
        var bytes = generateBytes(1_000_000);

        var response = sendFile(bytes);
        assertEquals(HttpResponseStatus.OK, response.status());

        var s3Bytes = s3Client.getObjectAsBytes(b -> b
            .bucket("test-bucket")
            .key(Integer.toString(issue.sequence.get() - 1))
        ).asByteArray();
        assertEquals(bytes.length, s3Bytes.length);
        assertArrayEquals(bytes, s3Bytes);
    }

    @Test
    public void streamWithChecksumValidation100Kb() {
        startIssue(true);
        var bytes = generateBytes(100_000);

        var response = sendFile(bytes);
        assertEquals(HttpResponseStatus.OK, response.status());

        var s3Bytes = s3Client.getObjectAsBytes(b -> b
            .bucket("test-bucket")
            .key(Integer.toString(issue.sequence.get() - 1))
        ).asByteArray();
        assertEquals(bytes.length, s3Bytes.length);
        assertArrayEquals(bytes, s3Bytes);
    }

    private void startIssue(boolean checksumValidation) {
        issue = new StreamToS3Issue(getUrl(), checksumValidation);
        issue.start();
    }

    private byte[] generateBytes(int size) {
        var bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    private HttpClientResponse sendFile(byte[] bytes) {
        return HttpClient.create()
            .headers(h -> h
                .set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream")
                .set(HttpHeaderNames.CONTENT_LENGTH, bytes.length)
            )
            .baseUrl("localhost:8080")
            .post()
            .uri("/file")
            .send(ByteBufFlux.fromInbound(Mono.just(bytes)))
            .response()
            .block();
    }

    private String getUrl() {
        return String.format("http://%s:%d", container.getContainerIpAddress(), container.getMappedPort(9090));
    }

    private S3Client getClient() {
        try {
            return S3Client.builder()
                .region(Region.EU_WEST_2)
                .endpointOverride(new URI(getUrl()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
        } catch (URISyntaxException var1) {
            throw new RuntimeException(var1);
        }
    }
}
