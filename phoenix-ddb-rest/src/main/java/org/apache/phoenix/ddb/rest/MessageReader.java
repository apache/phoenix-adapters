package org.apache.phoenix.ddb.rest;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.hbase.thirdparty.javax.ws.rs.Consumes;
import org.apache.hbase.thirdparty.javax.ws.rs.WebApplicationException;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MediaType;
import org.apache.hbase.thirdparty.javax.ws.rs.core.MultivaluedMap;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.MessageBodyReader;
import org.apache.hbase.thirdparty.javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.phoenix.ddb.rest.util.Constants;

@Provider
@Consumes({Constants.APPLICATION_AMZ_JSON, MediaType.APPLICATION_JSON})
public class MessageReader implements MessageBodyReader<Object> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations,
                            MediaType mediaType) {
    return type.isAssignableFrom(Map.class);
  }

  @Override
  public Object readFrom(Class<Object> type, Type genericType, Annotation[] annotations,
                         MediaType mediaType, MultivaluedMap<String, String> httpHeaders,
                         InputStream entityStream) throws IOException, WebApplicationException {
    return MAPPER.readValue(entityStream, type);
  }

}
