package pt.isel.ngspipes.engine_core.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class JacksonUtils {

    public static List<String> readPropertyValues(String content, String propertyName) throws IOException {
        List<String> values = new LinkedList<>();
        ObjectMapper mapper = getObjectMapper();
        JsonNode node = mapper.readValue(content, JsonNode.class);

        if (node instanceof ArrayNode) {
            ArrayNode arr = (ArrayNode)node;
            for (JsonNode insideNode : arr) {
                if (insideNode.has(propertyName))
                    values.add(insideNode.get(propertyName).textValue());
            }
        } else {
            if (node.has(propertyName))
                values.add(node.get(propertyName).textValue());
        }

        return values;
    }

    public static String serialize(Object obj) throws IOException {
        ObjectMapper mapper = getObjectMapper();
        return mapper.writeValueAsString(obj);
    }

    public static <T> T deserialize(String content, Class<T> klass) throws IOException {
        ObjectMapper mapper = getObjectMapper();
        return mapper.readValue(content, klass);
    }

    private static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }
}
