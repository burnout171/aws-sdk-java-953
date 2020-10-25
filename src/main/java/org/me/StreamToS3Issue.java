package org.me;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class StreamToS3Issue {

    private static final Logger log = LoggerFactory.getLogger(StreamToS3Issue.class);
    private final S3AsyncClient s3AsyncClient;
    private final HttpServer httpServer;

    private DisposableServer disposableServer;
    public AtomicInteger sequence = new AtomicInteger(1);

    public StreamToS3Issue(String url, boolean checksumValidation) {
        try {
            this.s3AsyncClient = createS3AsyncClient(url, checksumValidation);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        var handler = createFileHandler();
        this.httpServer = createHttpServer(handler);
    }

    /**
     * Start HTTP server.
     */
    public synchronized void start() {
        if (this.disposableServer == null) {
            this.disposableServer = this.httpServer.bindNow();
        }
    }

    /**
     * Stop HTTP server.
     */
    public synchronized void stop() {
        if (this.disposableServer != null) {
            this.disposableServer.disposeNow();
            this.disposableServer = null;
        }
    }

    /**
     * Configure S3AsyncClient.
     * @param url s3 url.
     * @param checksumValidation enable flag for checksum validation feature.
     * @return S3AsyncClient.
     * @throws URISyntaxException in case broken url.
     */
    private S3AsyncClient createS3AsyncClient(String url, boolean checksumValidation) throws URISyntaxException {
        var credentials = AwsBasicCredentials.create("test", "test");
        var s3Url = new URI(url);
        return S3AsyncClient.builder()
            .credentialsProvider(() -> credentials)
            .region(Region.EU_WEST_2)
            .endpointOverride(s3Url)
            .serviceConfiguration(
                S3Configuration.builder()
                    .checksumValidationEnabled(checksumValidation)
                    .build()
            )
            .httpClient(
                NettyNioAsyncHttpClient.builder()
                    .protocol(Protocol.HTTP1_1)
                    .build()
            )
            .build();
    }

    /**
     * Create callback with file streaming to s3.
     * @return callback.
     */
    private BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> createFileHandler() {
        return (request, response) -> {
            log.info("process.in");
            // prepare s3 request
            var s3Request = PutObjectRequest.builder()
                .bucket("test-bucket")
                .key(Integer.toString(sequence.getAndIncrement()))
                .contentType(request.requestHeaders().get("Content-Type"))
                .contentLength(Long.parseLong(request.requestHeaders().get("Content-Length")))
                .build();
            // prepare flux with ByteBuffer
            var byteBufferFlux = request.receive().asByteBuffer();
            var s3RequestBody = AsyncRequestBody.fromPublisher(byteBufferFlux);
            return Mono.fromCompletionStage(() -> s3AsyncClient.putObject(s3Request, s3RequestBody)) //upload ByteBuffers to s3
                .flatMap(r -> {
                    log.info("process.out");
                    return response.status(200)
                        .then();
                })
                .onErrorResume(e -> {
                    log.error("process.error", e);
                    return response.status(500)
                        .sendString(Mono.just(e.getMessage()))
                        .then();
                });
        };
    }

    /**
     * Configure reactor netty HTTP server.
     * @param fileHandler callback with logic
     * @return HttpServer
     */
    private HttpServer createHttpServer(BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> fileHandler) {
        return HttpServer.create()
            .host("localhost")
            .port(8080)
            .protocol(HttpProtocol.HTTP11)
            .route(routes -> routes.post("/file", fileHandler));
    }

}
