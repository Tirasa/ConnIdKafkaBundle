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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import net.tirasa.connid.bundles.kafka.serialization.AttributeDeserializer;
import net.tirasa.connid.bundles.kafka.serialization.AttributeSerializer;
import net.tirasa.connid.bundles.kafka.serialization.GuardedStringDeserializer;
import net.tirasa.connid.bundles.kafka.serialization.GuardedStringSerializer;
import net.tirasa.connid.bundles.kafka.serialization.SyncTokenDeserializer;
import net.tirasa.connid.bundles.kafka.serialization.SyncTokenSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.operations.APIOperation;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.PreconditionFailedException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.OperationOptionInfo;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;

@ConnectorClass(configurationClass = KafkaConfiguration.class, displayNameKey = "kafka.connector.display")
public class KafkaConnector
        implements Connector, CreateOp, UpdateOp, DeleteOp, SchemaOp, SyncOp, TestOp {

    private static final Log LOG = Log.getLog(KafkaConnector.class);

    public static final JsonMapper MAPPER;

    static {
        SimpleModule pojoModule = new SimpleModule("KafkaConnectorModule", new Version(1, 0, 0, null, null, null));
        pojoModule.addSerializer(GuardedString.class, new GuardedStringSerializer());
        pojoModule.addSerializer(Attribute.class, new AttributeSerializer());
        pojoModule.addSerializer(SyncToken.class, new SyncTokenSerializer());
        pojoModule.addDeserializer(GuardedString.class, new GuardedStringDeserializer());
        pojoModule.addDeserializer(Attribute.class, new AttributeDeserializer());
        pojoModule.addDeserializer(SyncToken.class, new SyncTokenDeserializer());

        MAPPER = JsonMapper.builder().
                addModule(pojoModule).
                addModule(new JavaTimeModule()).
                disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).
                visibility(PropertyAccessor.ALL, Visibility.NONE).
                visibility(PropertyAccessor.FIELD, Visibility.ANY).
                build();
    }

    private KafkaConfiguration configuration;

    private KafkaProducer<String, Object> producer;

    @Override
    public KafkaConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(final Configuration configuration) {
        this.configuration = (KafkaConfiguration) configuration;
        this.configuration.validate();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configuration.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, this.configuration.getClientId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.configuration.getValueSerializerClassName());
        try {
            producer = new KafkaProducer<>(props);
        } catch (KafkaException e) {
            LOG.error(e, "While creating Kafka producer");
            throw new ConnectionFailedException(e);
        }

        LOG.ok("Connector {0} successfully inited", getClass().getName());
    }

    @Override
    public void dispose() {
        try {
            Optional.ofNullable(producer).ifPresent(KafkaProducer::close);
        } catch (KafkaException e) {
            LOG.error(e, "While closing Kafka producer");
            throw new ConnectorException(e);
        }
    }

    private KafkaConsumer<String, Object> createConsumer(final ObjectClass objectClass) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configuration.getBootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, this.configuration.getClientId());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.configuration.getConsumerGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.configuration.getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.configuration.getValueDeserializerClassName());

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(getTopic(objectClass)));
        return consumer;
    }

    @Override
    public void test() {
        if (producer == null) {
            throw new ConnectorException("No Kafka producer configured");
        }
        try {
            producer.clientInstanceId(Duration.ofSeconds(10));
        } catch (Exception e) {
            LOG.error(e, "While testing Kafka producer");
            throw new ConnectionFailedException(e);
        }

        try (KafkaConsumer<String, Object> consumer = createConsumer(ObjectClass.ACCOUNT)) {
            consumer.clientInstanceId(Duration.ofSeconds(10));
        } catch (Exception e) {
            LOG.error(e, "While testing Kafka consumer");
            throw new ConnectionFailedException(e);
        }
    }

    @Override
    public Schema schema() {
        return new Schema(
                Collections.<ObjectClassInfo>emptySet(),
                Collections.<OperationOptionInfo>emptySet(),
                Collections.<Class<? extends APIOperation>, Set<ObjectClassInfo>>emptyMap(),
                Collections.<Class<? extends APIOperation>, Set<OperationOptionInfo>>emptyMap());
    }

    private String getTopic(final ObjectClass objectClass) {
        if (ObjectClass.ACCOUNT.equals(objectClass)) {
            return configuration.getAccountTopic();
        }
        if (ObjectClass.GROUP.equals(objectClass)) {
            return configuration.getGroupTopic();
        }
        if (ObjectClass.ALL.equals(objectClass)) {
            return configuration.getAllTopic();
        }
        throw new PreconditionFailedException("Unsupported object class: " + objectClass.getObjectClassValue());
    }

    @Override
    public Uid create(
            final ObjectClass objectClass,
            final Set<Attribute> createAttributes,
            final OperationOptions options) {

        Uid uid = new Uid(Optional.ofNullable(AttributeUtil.getNameFromAttributes(createAttributes)).
                orElseThrow(() -> new ConnectorException(Name.NAME + " not found in create attributes")).
                getNameValue());

        SyncDelta syncDelta = new SyncDeltaBuilder().
                setDeltaType(SyncDeltaType.CREATE).
                setObjectClass(objectClass).
                setUid(uid).
                setObject(new ConnectorObjectBuilder().addAttributes(createAttributes).setUid(uid).build()).
                setToken(new SyncToken(System.currentTimeMillis())).
                build();
        try {
            producer.send(new ProducerRecord<>(getTopic(objectClass), syncDelta));
        } catch (Exception e) {
            throw new ConnectorException("Could not send the create event to " + getTopic(objectClass), e);
        }

        return uid;
    }

    @Override
    public Uid update(
            final ObjectClass objectClass,
            final Uid uid,
            final Set<Attribute> replaceAttributes,
            final OperationOptions options) {

        SyncDelta syncDelta = new SyncDeltaBuilder().
                setDeltaType(SyncDeltaType.UPDATE).
                setObjectClass(objectClass).
                setUid(uid).
                setObject(new ConnectorObjectBuilder().addAttributes(replaceAttributes).setUid(uid).build()).
                setToken(new SyncToken(System.currentTimeMillis())).
                build();
        try {
            producer.send(new ProducerRecord<>(getTopic(objectClass), syncDelta));
        } catch (Exception e) {
            throw new ConnectorException("Could not send the update event to " + getTopic(objectClass), e);
        }

        return uid;
    }

    @Override
    public void delete(
            final ObjectClass objectClass,
            final Uid uid,
            final OperationOptions options) {

        SyncDelta syncDelta = new SyncDeltaBuilder().
                setDeltaType(SyncDeltaType.DELETE).
                setObjectClass(objectClass).
                setUid(uid).
                setToken(new SyncToken(System.currentTimeMillis())).
                build();
        try {
            producer.send(new ProducerRecord<>(getTopic(objectClass), syncDelta));
        } catch (Exception e) {
            throw new ConnectorException("Could not send the delete event to " + getTopic(objectClass), e);
        }
    }

    @Override
    public void sync(
            final ObjectClass objectClass,
            final SyncToken token,
            final SyncResultsHandler handler,
            final OperationOptions options) {

        try (KafkaConsumer<String, Object> consumer = createConsumer(objectClass)) {
            consumer.poll(Duration.ofMillis(configuration.getConsumerPollMillis())).forEach(record -> {
                LOG.ok("Processing {0} for topic {1}", record.key(), record.topic());

                Uid uid = new Uid(Optional.ofNullable(record.key()).orElseGet(() -> UUID.randomUUID().toString()));
                Map<String, String> headers = new HashMap<>();
                record.headers().forEach(header -> headers.put(header.key(), new String(header.value())));

                handler.handle(new SyncDeltaBuilder().
                        setDeltaType(SyncDeltaType.CREATE_OR_UPDATE).
                        setObjectClass(objectClass).
                        setUid(uid).
                        setObject(new ConnectorObjectBuilder().
                                addAttribute(new Name(uid.getUidValue())).
                                addAttribute(AttributeBuilder.build("record.headers", headers)).
                                addAttribute(AttributeBuilder.build("record.value", record.value())).
                                setUid(uid).
                                build()).
                        setToken(new SyncToken(record.timestamp())).
                        build());
            });
        } catch (Exception e) {
            throw new ConnectorException("While polling events from " + getTopic(objectClass), e);
        }
    }

    @Override
    public SyncToken getLatestSyncToken(final ObjectClass objectClass) {
        return new SyncToken(System.currentTimeMillis());
    }
}
