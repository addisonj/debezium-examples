package io.debezium.examples.kinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.MemoryDatabaseHistory;
import io.debezium.util.Clock;

/**
 * Demo for using the Debezium Embedded API to send change events to Amazon Kinesis with the KPL.
 */
public class KplDataSender implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataSender.class);

    private static final String APP_NAME = "kinesis";
    private static final String KINESIS_REGION_CONF_NAME = "kinesis.region";

    private final Configuration config;
    private final EmbeddedEngine.ChangeConsumer kplConsumer;


    class KplChangeConsumer implements EmbeddedEngine.ChangeConsumer {
        private final KinesisProducer kinesisClient;
        private final JsonConverter valueConverter;

        public KplChangeConsumer(Configuration config) {
            final String regionName = config.getString(KINESIS_REGION_CONF_NAME);

            final AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider("default");
            final KinesisProducerConfiguration kplConfig = new KinesisProducerConfiguration();
            kplConfig.setRegion(regionName);
            kplConfig.setCredentialsProvider(credentialsProvider);

            kinesisClient = new KinesisProducer(kplConfig);
            valueConverter = new JsonConverter();
            valueConverter.configure(config.asMap(), false);
        }

        @Override
        public void handleBatch(EmbeddedEngine.RecordCommitter committer, List<SourceRecord> records) throws Exception {
            ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
            records.forEach((record) -> {
                if (record.topic().equals(APP_NAME)) {
                    return;
                }

                Schema schema = SchemaBuilder.struct()
                        .field("key", record.keySchema())
                        .field("value", record.valueSchema())
                        .build();

                Struct message = new Struct(schema);
                message.put("key", record.key());
                message.put("value", record.value());

                String partitionKey = String.valueOf(record.key() != null ? record.key().hashCode() : -1);
                final byte[] payload = valueConverter.fromConnectData("dummy", schema, message);

                ListenableFuture<UserRecordResult> res = kinesisClient.addUserRecord(streamNameMapper(record.topic()), partitionKey, ByteBuffer.wrap(payload));
                CompletableFuture<Void> cf = convertToCompletable(res).thenApply((a) -> {
                    // TODO: figure out if I can call markProcessed here or if I need to
                    // call it in order, this is easily done by just having the future result
                    // contain the record
                    try {
                        committer.markProcessed(record);
                    }
                    catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                    return null;
                });
                futures.add(cf);
            });

            // if I call get() here, I only process a chunk at a time, and get more ordering
            // however, I can also process multiple chunks by collecting the futures and not flushing
            // until all the batches are finished
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply((v) -> {
                committer.markBatchFinished();
                return null;
            }).get();
        }

        private String streamNameMapper(String topic) {
            return topic;
        }


        private CompletableFuture<UserRecordResult> convertToCompletable(ListenableFuture<UserRecordResult> f) {
            CompletableFuture<UserRecordResult> completable = new CompletableFuture<UserRecordResult>() {
                @Override
                public boolean cancel(boolean mayInterupt) {
                    boolean res = f.cancel(mayInterupt);
                    super.cancel(mayInterupt);
                    return res;
                }
            };


            Futures.addCallback(
                    f,
                    new FutureCallback<UserRecordResult>() {
                        @Override
                        public void onSuccess(UserRecordResult t) {
                            completable.complete(t);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            completable.completeExceptionally(t);
                        }

                    });
            return completable;
        }
    }

    public KplDataSender() {
        config = Configuration.empty().withSystemProperties(Function.identity()).edit()
                .with(EmbeddedEngine.CONNECTOR_CLASS, "io.debezium.connector.mysql.MySqlConnector")
                .with(EmbeddedEngine.ENGINE_NAME, APP_NAME)
                .with(MySqlConnectorConfig.SERVER_NAME,APP_NAME)
                .with(MySqlConnectorConfig.SERVER_ID, 8192)

                // for demo purposes let's store offsets and history only in memory
                .with(EmbeddedEngine.OFFSET_STORAGE, "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with(MySqlConnectorConfig.DATABASE_HISTORY, MemoryDatabaseHistory.class.getName())

                // Send JSON without schema
                .with("schemas.enable", false)
                .build();

        kplConsumer = new KplChangeConsumer(config);
    }

    @Override
    public void run() {
        final EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .using(this.getClass().getClassLoader())
                .using(Clock.SYSTEM)
                .notifying(this.kplConsumer)
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Requesting embedded engine to shut down");
            engine.stop();
        }));

        awaitTermination(executor);
    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.info("Waiting another 10 seconds for the embedded engine to shut down");
            }
        }
        catch (InterruptedException e) {
            Thread.interrupted();
        }
    }


    public static void main(String[] args) {
        new ChangeDataSender().run();
    }
}
