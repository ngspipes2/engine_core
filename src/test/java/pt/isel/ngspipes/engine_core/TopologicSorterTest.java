package pt.isel.ngspipes.engine_core;

public class TopologicSorterTest {
//
//    @Before
//    public void before() {
//        ToolsRepositoryFactory.registerFactory(LocalToolsRepository::create);
//        ToolsRepositoryFactory.registerFactory(GithubToolsRepository::create);
//    }
//
//    @Test
//    public void topologicSorterParallelTest() {
//        URL path = ClassLoader.getSystemClassLoader().getResource("./pipeline.pipes");
//        NGSPipesParser parser = new NGSPipesParser();
//
//        try {
//            assert path != null;
//            String pipelineDescriptorContent = readContent(path.getPath());
//            IPipelineDescriptor pipelineDescriptor = parser.getFromString(pipelineDescriptorContent);
//            Arguments arguments = new Arguments("", true);
//            Pipeline pipeline = JobFactory.create("abc", pipelineDescriptor, new HashMap<>(), arguments, "");
//            List<ExecutionNode> graph = (List<ExecutionNode>) TopologicSorter.parallelSort(pipeline, pipelineDescriptor);
//            assertTrue(!graph.isEmpty());
//            assertEquals(2, graph.size());
//            Job blastxStep = graph.get(1).getChilds().get(0).getJob();
//            List<String> parents = (List<String>) blastxStep.getParents();
//            assertEquals("blastx", blastxStep.getId());
//            assertEquals(2, parents.size());
//            assertEquals("makeblastdb", parents.get(0));
//
//        } catch (ParserException | IOException | EngineException e) {
//            fail("shouldn't throw exception" + e.getMessage());
//        }
//    }
//
//    @Test
//    public void topologicSorterSequencialTest() {
//        URL path = ClassLoader.getSystemClassLoader().getResource("./pipeline.pipes");
//        NGSPipesParser parser = new NGSPipesParser();
//
//        try {
//            assert path != null;
//            String pipelineDescriptorContent = readContent(path.getPath());
//            IPipelineDescriptor pipelineDescriptor = parser.getFromString(pipelineDescriptorContent);
//            Arguments arguments = new Arguments("", false);
//            Pipeline pipeline = JobFactory.create("abc", pipelineDescriptor, new HashMap<>(), arguments, "");
//            List<ExecutionNode> graph = (List<ExecutionNode>) TopologicSorter.sequentialSort(pipeline, pipelineDescriptor);
//            assertEquals(5, graph.size());
//            assertEquals("trimmomatic", graph.get(0).getJob().getId());
//            assertEquals("makeblastdb", graph.get(1).getJob().getId());
//            assertEquals("velveth", graph.get(2).getJob().getId());
//            assertEquals("velvetg", graph.get(3).getJob().getId());
//            assertEquals("blastx", graph.get(4).getJob().getId());
//
//        } catch (ParserException | IOException | EngineException e) {
//            fail("shouldn't throw exception" + e.getMessage());
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
//
//        return sb.toString();
//    }

}
