package pt.isel.ngspipes.engine_core;

public class PipelineCommandBuilderTest {
//
//    @Before
//    public void before() {
//        ToolsRepositoryFactory.registerFactory(LocalToolsRepository::create);
//        ToolsRepositoryFactory.registerFactory(GithubToolsRepository::create);
//    }
//
//    @Test
//    public void dockerCommandBuilderPipeline() {
//        URL path = ClassLoader.getSystemClassLoader().getResource("./pipeline.pipes");
//        NGSPipesParser parser = new NGSPipesParser();
//
//        try {
//            assert path != null;
//            String pipelineDescriptorContent = readContent(path.getPath());
//            IPipelineDescriptor pipelineDescriptor = parser.getFromString(pipelineDescriptorContent);
//            Arguments arguments = new Arguments("", true);
//            Map<String, Object> parameters = new HashMap<>();
//            parameters.put("blastx_out", "out");
//            Pipeline pipeline = JobFactory.create("abc", pipelineDescriptor, parameters, arguments, "");
//            String context = "Docker";
//            ICommandBuilder builder = CommandBuilderSupplier.getCommandBuilder(context);
//
//            assertTrue(builder instanceof DockerCommandBuilder);
//
//            for (Job stepCtx : pipeline.getJobs()) {
//                String command = builder.build(pipeline, stepCtx.getId());
//                assertEquals("", command);
//            }
//
//        } catch (CommandBuilderException | ParserException | IOException | EngineException e) {
//            fail("shouldn't throw exception: " + e.getMessage());
//        }
//    }
//
//
//
//    private static String readContent(String filePath) throws IOException {
//        StringBuilder sb = new StringBuilder();
//        BufferedReader br = null;
//        String str;
//        try {
//            br = new BufferedReader(new FileReader(filePath));
//            while((str = br.readLine()) != null) {
//                sb.append(str);
//                sb.append("\n");
//            }
//        } finally {
//            if (br != null)
//                br.close();
//        }
//        return sb.toString();
//    }

}
