import org.junit.Before;
import org.junit.Test;
import pt.isel.ngspipes.dsl_core.descriptors.tool.ToolsRepositoryFactory;
import pt.isel.ngspipes.dsl_core.descriptors.tool.repository.GithubToolsRepository;
import pt.isel.ngspipes.dsl_core.descriptors.tool.repository.LocalToolsRepository;
import pt.isel.ngspipes.dsl_parser.domain.NGSPipesParser;
import pt.isel.ngspipes.dsl_parser.transversal.ParserException;
import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.ExecutionNode;
import pt.isel.ngspipes.engine_core.entities.JobUnit;
import pt.isel.ngspipes.engine_core.entities.Pipeline;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.utils.PipelineFactory;
import pt.isel.ngspipes.engine_core.utils.TopologicSorter;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class TopologicSorterTest {

    @Before
    public void before() {
        ToolsRepositoryFactory.registerFactory(LocalToolsRepository::create);
        ToolsRepositoryFactory.registerFactory(GithubToolsRepository::create);
    }

    @Test
    public void topologicSorterParallelTest() {
        URL path = ClassLoader.getSystemClassLoader().getResource("./pipeline.pipes");
        NGSPipesParser parser = new NGSPipesParser();

        try {
            assert path != null;
            String pipelineDescriptorContent = readContent(path.getPath());
            IPipelineDescriptor pipelineDescriptor = parser.getFromString(pipelineDescriptorContent);
            Pipeline pipeline = PipelineFactory.create(pipelineDescriptor, new HashMap<>());
            Arguments arguments = new Arguments("", true);
            pipeline = PipelineFactory.create(pipeline, PipelineFactory.createJobs(pipeline, arguments));
            List<ExecutionNode> graph = (List<ExecutionNode>) TopologicSorter.parallelSort(pipeline);
            assertTrue(!graph.isEmpty());
            assertEquals(2, graph.size());
            JobUnit blastxJobUnit = graph.get(1).getChilds().get(0).getJob();
            List<JobUnit> parents = (List<JobUnit>) blastxJobUnit.getParents();
            assertEquals("blastx", blastxJobUnit.getId());
            assertEquals(2, parents.size());
            assertEquals("makeblastdb", parents.get(0).getId());

        } catch (ParserException | IOException | EngineException e) {
            fail("shouldn't throw exception" + e.getMessage());
        }
    }

    @Test
    public void topologicSorterSequencialTest() {
        URL path = ClassLoader.getSystemClassLoader().getResource("./pipeline.pipes");
        NGSPipesParser parser = new NGSPipesParser();

        try {
            assert path != null;
            String pipelineDescriptorContent = readContent(path.getPath());
            IPipelineDescriptor pipelineDescriptor = parser.getFromString(pipelineDescriptorContent);
            Pipeline pipeline = PipelineFactory.create(pipelineDescriptor, new HashMap<>());
            Arguments arguments = new Arguments("", true);
            pipeline = PipelineFactory.create(pipeline, PipelineFactory.createJobs(pipeline, arguments));
            List<ExecutionNode> graph = (List<ExecutionNode>) TopologicSorter.sequentialSort(pipeline);
            assertEquals(5, graph.size());
            assertEquals("trimmomatic", graph.get(0).getJob().getId());
            assertEquals("makeblastdb", graph.get(1).getJob().getId());
            assertEquals("velveth", graph.get(2).getJob().getId());
            assertEquals("velvetg", graph.get(3).getJob().getId());
            assertEquals("blastx", graph.get(4).getJob().getId());

        } catch (ParserException | IOException | EngineException e) {
            fail("shouldn't throw exception" + e.getMessage());
        }
    }



    private static String readContent(String filePath) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        String str;
        try {
            br = new BufferedReader(new FileReader(filePath));
            while((str = br.readLine()) != null) {
                sb.append(str);
                sb.append("\n");
            }
        } finally {
            if (br != null)
                br.close();
        }

        return sb.toString();
    }

}
