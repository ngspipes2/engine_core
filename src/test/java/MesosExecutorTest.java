import org.junit.Before;
import org.junit.Test;
import pt.isel.ngspipes.dsl_parser.domain.NGSPipesParser;
import pt.isel.ngspipes.dsl_parser.transversal.ParserException;
import pt.isel.ngspipes.engine_common.entities.ConsoleArguments;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.executionReporter.ConsoleReporter;
import pt.isel.ngspipes.engine_common.interfaces.IExecutor;
import pt.isel.ngspipes.engine_core.entities.Status;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.implementations.Engine;
import pt.isel.ngspipes.engine_core.interfaces.IEngine;
import pt.isel.ngspipes.engine_executor.entities.MesosInfo;
import pt.isel.ngspipes.engine_executor.implementations.MesosExecutor;
import pt.isel.ngspipes.engine_executor.implementations.VagrantExecutor;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MesosExecutorTest {


//    static final String pipes = "E:/Work/NGSPipes/ngspipes2/engine_local/src/test/resources/pipelineSplitJoin.pipes";
//static final String pipes = "E:/Work/NGSPipes/ngspipes2/engine_local/src/test/resources/pipeline.pipes";
    static final String pipes = "E:/Work/NGSPipes/ngspipes2/engine_local/src/test/resources/pipelineSpread.pipes";
    static final String outPath = "E:/Work/NGSPipes/ngspipes2/engine_local/src/test/resources/outputs";
    static final String workingDirectory = "/home/calmen/pipes";

//    MesosInfo mesosInfo = new MesosInfo("10.141.141.11", "vagrant", "vagrant",
//            22, "http://10.141.141.11:4400", "/home/vagrant",
//            "E:\\Escola\\ISEL\\MEIC\\56_Semestre_Dissertacao\\vagrantMesos\\.vagrant\\machines\\ngspipes2\\virtualbox\\private_key");
    MesosInfo mesosInfo = new MesosInfo("10.62.73.5", "calmen", "ngs##19",
            22, "http://10.62.73.5:4400", workingDirectory);
    IExecutor executor = new MesosExecutor(new ConsoleReporter(), mesosInfo);
    IEngine engine = new Engine(executor);
    IPipelineDescriptor pipelineDescriptor;
    Map<String, Object> parameters;
    ConsoleArguments arguments;

    @Before
    public void init() throws IOException, ParserException {
        arguments = new ConsoleArguments(pipes, workingDirectory, outPath, 4, 6144, 0, false, parameters);
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
        long init = System.currentTimeMillis();
        try {
            Pipeline pipeline = engine.execute(pipelineDescriptor, parameters, arguments);
            do {
                if (pipeline.getState().getState().equals(StateEnum.SUCCESS))
                    break;
                if (pipeline.getState().getState().equals(StateEnum.FAILED))
                    break;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {

                }
            } while(true);
            String execId = pipeline.getName();
            Status status = engine.getStatus(execId);
            engine.getPipelineOutputs(execId, outPath);
        } catch (EngineException e) {
            System.out.println(e.getMessage());
        }
        System.out.println((System.currentTimeMillis() - init) * 0.000016667);
    }


    @Test
    public void parallelTest() {
        long init = System.currentTimeMillis();
        try {
            arguments.parallel = true;
            Pipeline pipeline = engine.execute(pipelineDescriptor, parameters, arguments);
            do {
                if (pipeline.getState().getState().equals(StateEnum.SUCCESS))
                    break;
                if (pipeline.getState().getState().equals(StateEnum.FAILED))
                    break;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
            } while(true);
        } catch (EngineException e) { }
        System.out.println((System.currentTimeMillis() - init) * 0.000016667);
    }
}
