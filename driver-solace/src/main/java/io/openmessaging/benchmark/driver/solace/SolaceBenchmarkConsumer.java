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
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceBenchmarkConsumer implements BenchmarkConsumer {

    private XMLMessageConsumer consumer;

    public SolaceBenchmarkConsumer(String topicName, JCSMPSession session, ConsumerCallback callback) throws JCSMPException {
        session.connect();
        Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
        consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                callback.messageReceived(msg.getBytes(), msg.getReceiveTimestamp());
            }
            @Override
            public void onException(JCSMPException e) {
                log.error("Consumer received exception: %s%n",e);
            }
        });
        session.addSubscription(topic);
        log.info("Connected. Awaiting message...");
        consumer.start();

    }
    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SolaceBenchmarkConsumer.class);
}
