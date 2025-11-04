/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.hbase.thirdparty.javax.ws.rs.Produces;
import org.apache.hbase.thirdparty.javax.ws.rs.WebApplicationException;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MultivaluedMap;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.MessageBodyWriter;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.phoenix.ddb.rest.util.Constants;

/**
 * Jersey provider that converts <code>Map</code>s to their JSON representation.
 */
@Provider
@Produces({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
public class MessageWriter implements MessageBodyWriter<Object> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations,
                               MediaType mediaType) {
        return Map.class.isAssignableFrom(aClass);
    }

    @Override
    public void writeTo(Object obj, Class<?> aClass, Type type, Annotation[] annotations,
                        MediaType mediaType,
                        MultivaluedMap<String, Object> stringObjectMultivaluedMap,
                        OutputStream outputStream) throws IOException, WebApplicationException {
        Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        MAPPER.writeValue(writer, obj);
    }

}
