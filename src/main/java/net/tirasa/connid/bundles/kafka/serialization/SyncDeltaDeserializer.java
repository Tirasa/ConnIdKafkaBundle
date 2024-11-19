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
package net.tirasa.connid.bundles.kafka.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import net.tirasa.connid.bundles.kafka.KafkaConnector;
import org.apache.kafka.common.serialization.Deserializer;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.SyncDelta;

public class SyncDeltaDeserializer implements Deserializer<SyncDelta> {

    private static final Log LOG = Log.getLog(SyncDeltaDeserializer.class);

    @Override
    public SyncDelta deserialize(final String topic, final byte[] data) {
        try {
            return KafkaConnector.MAPPER.readValue(new ByteArrayInputStream(data), SyncDelta.class);
        } catch (IOException e) {
            LOG.error(e, "While deserializing {0}", new String(data));
            return null;
        }
    }
}
