/*
 */
package io.openmessaging.benchmark.driver.solace;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolaceBenchmarkDriver implements BenchmarkDriver {

    private SolaceConfig config;
    private JCSMPSession session;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, SolaceConfig.class);
        log.info("Read config file: " + config.toString());
        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, config.host);     // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME, config.vpn); // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, config.username); // client-username
        properties.setProperty(JCSMPProperties.PASSWORD, config.password); // client-password
        try {
            session =  JCSMPFactory.onlyInstance().createSession(properties);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "solace";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int numOfPartitions) {
        log.info("ignored partitions");
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        return future;
    }

    @Override
    public CompletableFuture<Void> notifyTopicCreation(String topic, int numOfPartitions) {
        return CompletableFuture.completedFuture(null); // no-op
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        try {
            return CompletableFuture.completedFuture(new SolaceBenchmarkProducer(topic, session));
        } catch (Exception e) {
            CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName,
                                                               Optional<Integer> partition, ConsumerCallback consumerCallback) {
        CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
        ForkJoinPool.commonPool().submit(() -> {
            try {
                // not using subscriptionName for now cause I'm not sure
                BenchmarkConsumer consumer = new SolaceBenchmarkConsumer(topic, session, consumerCallback);
                future.complete(consumer);
            } catch (Exception e) { // TODO finer grain exception
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    @Override
    public void close() throws Exception {
        log.info("Shutting down Solace benchmark driver");
        if (session != null) {
            session.closeSession();
        }
        log.info("Solace benchmark driver successfully shut down");
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Logger log = LoggerFactory.getLogger(SolaceBenchmarkDriver.class);
}
