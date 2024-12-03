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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.UUID;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.jupiter.api.Test;

class SyncDeltaSerDesTests {

    @Test
    void serdes() {
        Uid uid = new Uid(UUID.randomUUID().toString());
        SyncDelta syncDelta = new SyncDeltaBuilder().
                setDeltaType(SyncDeltaType.CREATE).
                setObjectClass(ObjectClass.ACCOUNT).
                setUid(uid).
                setObject(new ConnectorObjectBuilder().addAttributes(Set.of(
                        new Name("name"),
                        AttributeBuilder.build("email", "connid-dev@googlegroups.com"))).
                        setUid(uid).build()).
                setToken(new SyncToken(System.currentTimeMillis())).
                build();

        byte[] serialized = new SyncDeltaSerializer().serialize("topic", syncDelta);
        SyncDelta deserialized = new SyncDeltaDeserializer().deserialize("topic", serialized);
        assertEquals(syncDelta, deserialized);
    }
}
