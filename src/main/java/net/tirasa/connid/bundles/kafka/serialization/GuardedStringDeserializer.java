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
import java.lang.reflect.Field;
import java.util.Base64;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.EncryptorFactory;
import org.identityconnectors.common.security.GuardedString;

public class GuardedStringDeserializer extends JsonDeserializer<GuardedString> {

    private static final Log LOG = Log.getLog(GuardedStringDeserializer.class);

    private static final String READONLY = "readOnly";

    private static final String DISPOSED = "disposed";

    private static final String ENCRYPTED_BYTES = "encryptedBytes";

    private static final String BASE64_SHA1_HASH = "base64SHA1Hash";

    private static final String LOG_ERROR_MESSAGE = "Could not set field value to {}";

    @Override
    public GuardedString deserialize(final JsonParser jp, final DeserializationContext ctx)
            throws IOException {

        ObjectNode tree = jp.readValueAsTree();

        boolean readOnly = false;
        if (tree.has(READONLY)) {
            readOnly = tree.get(READONLY).asBoolean();
        }
        boolean disposed = false;
        if (tree.has(DISPOSED)) {
            disposed = tree.get(DISPOSED).asBoolean();
        }
        byte[] encryptedBytes = null;
        if (tree.has(ENCRYPTED_BYTES)) {
            encryptedBytes = Base64.getDecoder().decode(tree.get(ENCRYPTED_BYTES).asText());
        }
        String base64SHA1Hash = null;
        if (tree.has(BASE64_SHA1_HASH)) {
            base64SHA1Hash = tree.get(BASE64_SHA1_HASH).asText();
        }

        final byte[] clearBytes = EncryptorFactory.getInstance().getDefaultEncryptor().decrypt(encryptedBytes);

        GuardedString dest = new GuardedString(new String(clearBytes).toCharArray());

        try {
            Field field = GuardedString.class.getDeclaredField(READONLY);
            field.setAccessible(true);
            field.set(dest, readOnly);
        } catch (Exception e) {
            LOG.error(LOG_ERROR_MESSAGE, readOnly, e);
        }

        try {
            Field field = GuardedString.class.getDeclaredField(DISPOSED);
            field.setAccessible(true);
            field.set(dest, disposed);
        } catch (Exception e) {
            LOG.error(LOG_ERROR_MESSAGE, disposed, e);
        }

        if (base64SHA1Hash != null) {
            try {
                Field field = GuardedString.class.getDeclaredField(BASE64_SHA1_HASH);
                field.setAccessible(true);
                field.set(dest, base64SHA1Hash);
            } catch (Exception e) {
                LOG.error(LOG_ERROR_MESSAGE, base64SHA1Hash, e);
            }
        }

        return dest;
    }

}
