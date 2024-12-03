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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Map;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;

public class ConnectorObjectDeserializer extends JsonDeserializer<ConnectorObject> {

    private static final AttributeDeserializer ATTR_DESERIALIZER = new AttributeDeserializer();

    @Override
    public ConnectorObject deserialize(final JsonParser jp, final DeserializationContext ctx) throws IOException {
        ObjectNode tree = jp.readValueAsTree();

        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

        if (tree.has("objectClass")) {
            builder.setObjectClass(new ObjectClass(tree.get("objectClass").get("type").asText()));
        }

        if (tree.has("attributeMap")) {
            JsonParser parser = tree.get("attributeMap").traverse();
            parser.setCodec(jp.getCodec());
            Map<String, ObjectNode> attributeMap = parser.readValueAs(new TypeReference<Map<String, ObjectNode>>() {
            });

            for (ObjectNode attribute : attributeMap.values()) {
                JsonParser p = attribute.traverse();
                p.setCodec(jp.getCodec());
                builder.addAttribute(ATTR_DESERIALIZER.deserialize(p, ctx));
            }
        }

        return builder.build();
    }
}
