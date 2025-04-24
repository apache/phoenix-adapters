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
