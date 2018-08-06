package pt.isel.ngspipes.engine_core;

import org.junit.Before;
import org.junit.Test;
import pt.isel.ngspipes.dsl_core.descriptors.tool.ToolsRepositoryFactory;
import pt.isel.ngspipes.dsl_core.descriptors.tool.repository.GithubToolsRepository;
import pt.isel.ngspipes.dsl_core.descriptors.tool.repository.LocalToolsRepository;
import pt.isel.ngspipes.dsl_parser.domain.NGSPipesParser;
import pt.isel.ngspipes.dsl_parser.transversal.ParserException;
import pt.isel.ngspipes.engine_core.entities.*;
import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.entities.contexts.StepContext;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.utils.ContextFactory;
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
            Arguments arguments = new Arguments("", true);
            PipelineContext pipeline = ContextFactory.create("abc", pipelineDescriptor, new HashMap<>(), arguments, "");
            List<ExecutionNode> graph = (List<ExecutionNode>) TopologicSorter.parallelSort(pipeline);
            assertTrue(!graph.isEmpty());
            assertEquals(2, graph.size());
            StepContext blastxStep = graph.get(1).getChilds().get(0).getStepContext();
            List<StepContext> parents = (List<StepContext>) blastxStep.getParents();
            assertEquals("blastx", blastxStep.getId());
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
            Arguments arguments = new Arguments("", false);
            PipelineContext pipeline = ContextFactory.create("abc", pipelineDescriptor, new HashMap<>(), arguments, "");
            List<ExecutionNode> graph = (List<ExecutionNode>) TopologicSorter.sequentialSort(pipeline);
            assertEquals(5, graph.size());
            assertEquals("trimmomatic", graph.get(0).getStepContext().getId());
            assertEquals("makeblastdb", graph.get(1).getStepContext().getId());
            assertEquals("velveth", graph.get(2).getStepContext().getId());
            assertEquals("velvetg", graph.get(3).getStepContext().getId());
            assertEquals("blastx", graph.get(4).getStepContext().getId());

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
