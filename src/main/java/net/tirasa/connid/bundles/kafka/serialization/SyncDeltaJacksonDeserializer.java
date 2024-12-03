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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.Uid;

public class SyncDeltaJacksonDeserializer extends JsonDeserializer<SyncDelta> {

    private static final AttributeDeserializer ATTR_DESERIALIZER = new AttributeDeserializer();

    private static final SyncTokenDeserializer SYNC_TOKEN_DESERIALIZER = new SyncTokenDeserializer();

    private static final ConnectorObjectDeserializer CONNECTOR_OBJECT_DESERIALIZER = new ConnectorObjectDeserializer();

    @Override
    public SyncDelta deserialize(final JsonParser jp, final DeserializationContext ctx) throws IOException {
        ObjectNode tree = jp.readValueAsTree();

        SyncDeltaBuilder builder = new SyncDeltaBuilder();

        if (tree.has("objectClass")) {
            builder.setObjectClass(new ObjectClass(tree.get("objectClass").get("type").asText()));
        }

        if (tree.has("uid")) {
            JsonParser parser = tree.get("uid").traverse();
            parser.setCodec(jp.getCodec());
            builder.setUid((Uid) ATTR_DESERIALIZER.deserialize(parser, ctx));
        }

        if (tree.has("previousUid") && !tree.get("previousUid").isNull()) {
            JsonParser parser = tree.get("previousUid").traverse();
            parser.setCodec(jp.getCodec());
            builder.setPreviousUid((Uid) ATTR_DESERIALIZER.deserialize(parser, ctx));
        }

        if (tree.has("token")) {
            JsonParser parser = tree.get("token").traverse();
            parser.setCodec(jp.getCodec());
            builder.setToken(SYNC_TOKEN_DESERIALIZER.deserialize(parser, ctx));
        }

        if (tree.has("deltaType")) {
            builder.setDeltaType(SyncDeltaType.valueOf(tree.get("deltaType").asText()));
        }

        if (tree.has("object")) {
            JsonParser parser = tree.get("object").traverse();
            parser.setCodec(jp.getCodec());
            builder.setObject(CONNECTOR_OBJECT_DESERIALIZER.deserialize(parser, ctx));
        }

        return builder.build();
    }
}
