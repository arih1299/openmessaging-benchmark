/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.solace;

import com.solacesystems.jcsmp.*;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class SolaceBenchmarkProducer implements BenchmarkProducer {

    private Topic topic;
    private XMLMessageProducer producer;

    public SolaceBenchmarkProducer(String topicName, JCSMPSession session) throws JCSMPException {
        session.connect();
        topic = JCSMPFactory.onlyInstance().createTopic(topicName);
        /** Anonymous inner-class for handling publishing events */
        producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                log.debug("Producer received response for msg: " + messageID);
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                log.error("Producer received error for msg: %s@%s - %s%n",
                        messageID,timestamp,e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> optional, byte[] bytes) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        msg.setText(bytes.toString());
        try {
            producer.send(msg, topic);
        } catch (JCSMPException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void close() throws Exception {
    }

    private static final Logger log = LoggerFactory.getLogger(SolaceBenchmarkProducer.class);
}
