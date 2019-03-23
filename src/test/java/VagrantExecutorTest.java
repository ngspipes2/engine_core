import entities.ConsoleArguments;
import executionReporter.ConsoleReporter;
import implementations.VagrantExecutor;
import interfaces.IExecutor;
import org.junit.Before;
import org.junit.Test;
import pt.isel.ngspipes.dsl_parser.domain.NGSPipesParser;
import pt.isel.ngspipes.dsl_parser.transversal.ParserException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VagrantExecutorTest {


    static final String pipes = "E:/Work/NGSPipes/ngspipes2/engine_local/src/test/resources/pipeline.pipes";
    //        static final String pipes = "E:/Work/NGSPipes/ngspipes2/engine_local/src/test/resources/pipelineSpread.pipes";
    static final String outPath = "E:/Work/NGSPipes/ngspipes2/engine_local/src/test/resources/outputs";

    IExecutor engine = new VagrantExecutor(new ConsoleReporter());
    IPipelineDescriptor pipelineDescriptor;
    Map<String, Object> parameters ;
    ConsoleArguments arguments;

    @Before
    public void init() throws IOException, ParserException {
        arguments = new ConsoleArguments(pipes, outPath, 4, 8192, 0, false, parameters);
        pipelineDescriptor = getPipelineDescriptor(arguments);
        parameters = new HashMap<String, Object>(){{
            put("blastx_out", "blast.out");
        }};
    }

    private static IPipelineDescriptor getPipelineDescriptor(ConsoleArguments arguments) throws IOException, ParserException {
        System.out.println("Arguments parsed");
        String pipesContent = readContent(arguments.pipes);
        NGSPipesParser parser = new NGSPipesParser();
        IPipelineDescriptor pipelineDescriptor = parser.getFromString(pipesContent);
        System.out.println("Pipeline loaded");
        return pipelineDescriptor;
    }


    private static String readContent(String filePath) throws IOException {
        StringBuilder sb = new StringBuilder();
        String str;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while ((str = br.readLine()) != null) {
                sb.append(str);
                sb.append("\n");
            }
        }
        return sb.toString();
    }


    @Test
    public void sequentialTest() {

    }


    @Test
    public void parallelTest() {

    }
}
