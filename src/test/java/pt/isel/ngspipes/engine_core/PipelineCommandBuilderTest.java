package pt.isel.ngspipes.engine_core;

import org.junit.Before;
import org.junit.Test;
import pt.isel.ngspipes.dsl_core.descriptors.tool.ToolsRepositoryFactory;
import pt.isel.ngspipes.dsl_core.descriptors.tool.repository.GithubToolsRepository;
import pt.isel.ngspipes.dsl_core.descriptors.tool.repository.LocalToolsRepository;
import pt.isel.ngspipes.dsl_parser.domain.NGSPipesParser;
import pt.isel.ngspipes.dsl_parser.transversal.ParserException;
import pt.isel.ngspipes.engine_core.commandBuilders.DockerCommandBuilder;
import pt.isel.ngspipes.engine_core.commandBuilders.ICommandBuilder;
import pt.isel.ngspipes.engine_core.entities.Arguments;
import pt.isel.ngspipes.engine_core.entities.contexts.PipelineContext;
import pt.isel.ngspipes.engine_core.entities.contexts.StepContext;
import pt.isel.ngspipes.engine_core.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_core.exception.EngineException;
import pt.isel.ngspipes.engine_core.utils.CommandBuilderSupplier;
import pt.isel.ngspipes.engine_core.utils.ContextFactory;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PipelineCommandBuilderTest {

    @Before
    public void before() {
        ToolsRepositoryFactory.registerFactory(LocalToolsRepository::create);
        ToolsRepositoryFactory.registerFactory(GithubToolsRepository::create);
    }

    @Test
    public void dockerCommandBuilderPipeline() {
        URL path = ClassLoader.getSystemClassLoader().getResource("./pipeline.pipes");
        NGSPipesParser parser = new NGSPipesParser();

        try {
            assert path != null;
            String pipelineDescriptorContent = readContent(path.getPath());
            IPipelineDescriptor pipelineDescriptor = parser.getFromString(pipelineDescriptorContent);
            Arguments arguments = new Arguments("", true);
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("blastx_out", "out");
            PipelineContext pipeline = ContextFactory.create("abc", pipelineDescriptor, parameters, arguments, "");
            String context = "Docker";
            ICommandBuilder builder = CommandBuilderSupplier.getCommandBuilder(context);

            assertTrue(builder instanceof DockerCommandBuilder);

            for (StepContext stepCtx : pipeline.getStepsContexts().values()) {
                String command = builder.build(pipeline, stepCtx.getId());
                assertEquals("", command);
            }

        } catch (CommandBuilderException | ParserException | IOException | EngineException e) {
            fail("shouldn't throw exception: " + e.getMessage());
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
