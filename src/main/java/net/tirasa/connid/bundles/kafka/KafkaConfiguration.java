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

import net.tirasa.connid.bundles.kafka.serialization.SyncDeltaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

public class KafkaConfiguration extends AbstractConfiguration {

    private String bootstrapServers;

    private String clientId;

    private String consumerGroupId;

    private String autoOffsetReset = "earliest";

    private String valueSerializerClassName = SyncDeltaSerializer.class.getName();

    private String valueDeserializerClassName = StringDeserializer.class.getName();

    private String accountTopic = ObjectClass.ACCOUNT_NAME;

    private String groupTopic = ObjectClass.GROUP_NAME;

    private String allTopic = ObjectClass.ALL_NAME;

    private long consumerPollMillis = 100;

    @ConfigurationProperty(displayMessageKey = "bootstrapServers.display",
            helpMessageKey = "bootstrapServers.help", required = true, order = 1)
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @ConfigurationProperty(displayMessageKey = "clientId.display",
            helpMessageKey = "clientId.help", required = true, order = 2)
    public String getClientId() {
        return clientId;
    }

    public void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    @ConfigurationProperty(displayMessageKey = "consumerGroupId.display",
            helpMessageKey = "consumerGroupId.help", required = true, order = 3)
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(final String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    @ConfigurationProperty(displayMessageKey = "autoOffsetReset.display",
            helpMessageKey = "autoOffsetReset.help", required = true, order = 4,
            allowedValues = { "earliest", "latest", "none" })
    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(final String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    @ConfigurationProperty(displayMessageKey = "valueSerializerClassName.display",
            helpMessageKey = "valueSerializerClassName.help", required = true, order = 5)
    public String getValueSerializerClassName() {
        return valueSerializerClassName;
    }

    public void setValueSerializerClassName(final String valueSerializerClassName) {
        this.valueSerializerClassName = valueSerializerClassName;
    }

    @ConfigurationProperty(displayMessageKey = "valueDeserializerClassName.display",
            helpMessageKey = "valueDeserializerClassName.help", required = true, order = 6)
    public String getValueDeserializerClassName() {
        return valueDeserializerClassName;
    }

    public void setValueDeserializerClassName(final String valueDeserializerClassName) {
        this.valueDeserializerClassName = valueDeserializerClassName;
    }

    @ConfigurationProperty(displayMessageKey = "accountTopic.display",
            helpMessageKey = "accountTopic.help", order = 7)
    public String getAccountTopic() {
        return accountTopic;
    }

    public void setAccountTopic(final String accountTopic) {
        this.accountTopic = accountTopic;
    }

    @ConfigurationProperty(displayMessageKey = "groupTopic.display",
            helpMessageKey = "groupTopic.help", order = 8)
    public String getGroupTopic() {
        return groupTopic;
    }

    public void setGroupTopic(final String groupTopic) {
        this.groupTopic = groupTopic;
    }

    @ConfigurationProperty(displayMessageKey = "allTopic.display",
            helpMessageKey = "allTopic.help", order = 9)
    public String getAllTopic() {
        return allTopic;
    }

    public void setAllTopic(final String allTopic) {
        this.allTopic = allTopic;
    }

    @ConfigurationProperty(displayMessageKey = "consumerPollMillis.display",
            helpMessageKey = "consumerPollMillis.help", order = 10)
    public long getConsumerPollMillis() {
        return consumerPollMillis;
    }

    public void setConsumerPollMillis(final long consumerPollMillis) {
        this.consumerPollMillis = consumerPollMillis;
    }

    @Override
    public void validate() {
        if (StringUtil.isBlank(bootstrapServers)) {
            throw new ConfigurationException("Missing " + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        }
        if (StringUtil.isBlank(clientId)) {
            throw new ConfigurationException("Missing " + ProducerConfig.CLIENT_ID_CONFIG);
        }
        if (StringUtil.isBlank(consumerGroupId)) {
            throw new ConfigurationException("Missing " + ConsumerConfig.GROUP_ID_CONFIG);
        }
        if (StringUtil.isBlank(autoOffsetReset)) {
            throw new ConfigurationException("Missing " + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        }
        if (StringUtil.isBlank(valueSerializerClassName)) {
            throw new ConfigurationException("Missing " + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }
        if (StringUtil.isBlank(valueDeserializerClassName)) {
            throw new ConfigurationException("Missing " + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }
    }
}
