/**
 * Copyright (C) 2024 ConnId (connid-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.tirasa.connid.bundles.kafka;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.LiveSyncDelta;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KafkaConnectorTests {

    @Container
    static ConfluentKafkaContainer KAFKA_CONTAINER = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.9.0")).
            withEnv("KAFKA_LISTENERS", "PLAINTEXT://:9092,BROKER://:9093,CONTROLLER://:9094");

    protected static KafkaConfiguration newConfiguration() {
        KafkaConfiguration config = new KafkaConfiguration();
        config.setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers());
        config.setClientId("connid-client");
        config.setConsumerGroupId("connid-test");
        return config;
    }

    protected static ConnectorFacade newFacade() {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        APIConfiguration impl = TestHelpers.createTestConfiguration(KafkaConnector.class, newConfiguration());
        impl.getResultsHandlerConfiguration().setFilteredResultsHandlerInValidationMode(true);
        return factory.newInstance(impl);
    }

    @Test
    void test() {
        newFacade().test();
    }

    @Test
    void create() {
        Uid created = newFacade().create(
                ObjectClass.ACCOUNT,
                Set.of(new Name("testcreate")),
                new OperationOptionsBuilder().build());
        assertNotNull(created);
    }

    @Test
    void update() {
        Uid uid = new Uid("testupdate");
        Uid updated = newFacade().update(
                ObjectClass.ACCOUNT,
                uid,
                Set.of(new Name("testupdate")),
                new OperationOptionsBuilder().build());
        assertEquals(uid, updated);
    }

    @Test
    void delete() {
        assertDoesNotThrow(() -> newFacade().delete(
                ObjectClass.ACCOUNT,
                new Uid("testdelete"),
                new OperationOptionsBuilder().build()));
    }

    @Test
    void livesync() throws InterruptedException, ExecutionException {
        List<LiveSyncDelta> deltas = new ArrayList<>();
        newFacade().livesync(
                ObjectClass.GROUP,
                deltas::add,
                new OperationOptionsBuilder().build());
        assertTrue(deltas.isEmpty());

        // create a producer and send a new event
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, getClass().getSimpleName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String value = "{\"key\":\"value\"}";
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(ObjectClass.GROUP_NAME, value)).get();
        }

        // live sync
        deltas.clear();
        await().atMost(5, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
            newFacade().livesync(
                    ObjectClass.GROUP,
                    deltas::add,
                    new OperationOptionsBuilder().build());
            return !deltas.isEmpty();
        });
        assertEquals(1, deltas.size());
        assertNotNull(deltas.get(0).getObject().getAttributeByName("record.timestamp").getValue().get(0));
        assertNull(deltas.get(0).getObject().getAttributeByName("record.headers"));
        assertEquals(value, deltas.get(0).getObject().getAttributeByName("record.value").getValue().get(0));
    }
}
