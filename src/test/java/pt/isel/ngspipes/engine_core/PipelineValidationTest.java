package pt.isel.ngspipes.engine_core;

public class PipelineValidationTest {
//
//    @Before
//    public void before() {
//        ToolsRepositoryFactory.registerFactory(LocalToolsRepository::create);
//        ToolsRepositoryFactory.registerFactory(GithubToolsRepository::create);
//    }
//
//
//    @Test
//    public void validatePipelineWithoutRequiredParameter() {
//
//    }
//
//    @Test
//    public void validatePipelineWithoutDependentRequiredParameter() {
//
//    }
//
//    @Test
//    public void validatePipeline() {
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
//            ValidateUtils.validateRepositories(pipelineDescriptor.getRepositories());
//            ValidateUtils.validateNonCyclePipeline(pipelineDescriptor, parameters);
//            ValidateUtils.validateJobs(pipelineDescriptor, parameters);
//            ValidateUtils.validateOutputs(pipelineDescriptor, parameters);
//
//        } catch (ParserException | IOException | EngineException e) {
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
