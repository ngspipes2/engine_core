package pt.isel.ngspipes.engine_core.entities.factories;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import pt.isel.ngspipes.engine_core.implementations.ChronosJobStatusDto;

import java.io.IOException;

public class ChronosJobStatusFactory {


    public static ChronosJobStatusDto getChronosJobStatusDto(String content) throws IOException {
        return getObjectMapper(new JsonFactory()).readValue(content, ChronosJobStatusDto[].class)[0];
    }

    private static ObjectMapper getObjectMapper(JsonFactory factory) {
        return new ObjectMapper(factory);
    }
}
