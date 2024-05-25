package org.aksw.maven.plugin.lsq;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.function.Function;

import org.aksw.commons.io.util.UriToPathUtils;
import org.aksw.commons.util.string.FileName;
import org.aksw.commons.util.string.FileNameParser;
import org.aksw.jena_sparql_api.conjure.datapod.api.RdfDataPod;
import org.aksw.jena_sparql_api.conjure.datapod.impl.DataPods;
import org.aksw.jena_sparql_api.conjure.dataref.rdf.api.RdfDataRefSparqlEndpoint;
import org.aksw.jena_sparql_api.conjure.utils.ContentTypeUtils;
import org.aksw.jenax.dataaccess.sparql.connection.reconnect.SparqlQueryConnectionWithReconnect;
import org.aksw.maven.plugin.lsq.FileUtilsBackport.OverwriteMode;
import org.aksw.simba.lsq.cli.cmd.base.CmdLsqRdfizeBase;
import org.aksw.simba.lsq.cli.main.MainCliLsq;
import org.aksw.simba.lsq.core.util.SkolemizeBackport;
import org.aksw.simba.lsq.enricher.benchmark.core.LsqBenchmarkProcessor;
import org.aksw.simba.lsq.enricher.core.LsqEnricherRegistry;
import org.aksw.simba.lsq.enricher.core.LsqEnricherShell;
import org.aksw.simba.lsq.model.ExperimentConfig;
import org.aksw.simba.lsq.model.ExperimentExec;
import org.aksw.simba.lsq.model.ExperimentRun;
import org.aksw.simba.lsq.model.LsqQuery;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.SparqlQueryConnection;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.RemoteRepository;

import io.reactivex.rxjava3.core.Flowable;

@Mojo(name = "benchmark", defaultPhase = LifecyclePhase.PACKAGE)
public class LsqBenchmarkMojo extends AbstractMojo {

    /** The repository system (Aether) which does most of the management. */
    @Component
    private RepositorySystem repoSystem;

    /** The current repository/network configuration of Maven. */
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    private RepositorySystemSession repoSession;

    /** The project's remote repositories to use for the resolution of project dependencies. */
    @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true)
    private List<RemoteRepository> projectRepos;

    /** The Maven project */
    @Parameter(defaultValue = "${project}", readonly = true)
    private MavenProject project;

    @Component
    private MavenProjectHelper mavenProjectHelper;

    /** The input log file that contains SPARQL statements */
    @Parameter
    private File logFile;

    /** The list of enrichers */
    @Parameter
    private List<String> enrichers = new ArrayList<>();

    @Parameter
    private String logFormat;

    /** Label for the dataset the benchmark is run on; will appear in IRIs */
    @Parameter(defaultValue = "${project.artifactId}")
    private String datasetId;

    /** Label for the dataset the benchmark is run on; will appear in IRIs */
    @Parameter(required = true)
    private String serviceUrl;

    @Parameter(required = true)
    private String baseIri;

    @Parameter(defaultValue = "1")
    private int warmupRuns;

    @Parameter // (defaultValue = "-1")
    private Integer warmupTaskLimit;


    @Parameter(defaultValue = "true") // Whether to log warmup runs (default true)
    private boolean warmupLog;

    // measurement runs
    @Parameter(defaultValue = "1")
    private int runs;

    @Parameter(defaultValue = "SELECT * { ?s a <http://www.example.org/Thing> }")
    protected String testQuery;

    @Parameter(property = "lsq.skip", defaultValue = "false")
    protected boolean skip;

    // Graph into which to add metadata about benchmark runs
//    @Parameter(property = "lsq.metaGraph", defaultValue = "http://lsq.aksw.org/meta")
//    protected String metaGraph;

    // protected int testQueryFrequency

    /** If an output file is specified then the tdb2 directory becomes a sibling of it */
    @Parameter // (defaultValue = "${project.build.directory}/lsqdb")
    private File resultTdbStore;

    private void initResultTdbStore() {
        if (resultTdbStore == null) {
            String dirName = "lsqdb";
            resultTdbStore = outputFile != null
                    ? outputFile.toPath().resolveSibling(dirName).toFile()
                    : buildDirectory.toPath().resolveSibling(dirName).toFile();
        }
    }

    @Parameter(defaultValue = "${project.build.directory}", readonly = true)
    private File buildDirectory;

//    @Parameter(required = true)
//    private String baseIri;



//    @Option(names = { "-e", "--endpoint" }, required = true,
//            description = "SPARQL endpoint URL on which to execute queries")
//    public String endpoint = null;
//
//    @Option(names = { "-g", "--default-graph" },
//            description = "Graph(s) to use as the default graph - i.e. plain triple patterns such as { ?s ?p ?o } will match on those graphs")
//    public List<String> defaultGraphs = new ArrayList<>();
//
//    @Option(names = { "--ct", "--connection-timeout" }, description = "Timeout in milliseconds")
//    public Long connectionTimeoutInMs = null;
//
//    @Option(names = { "--qt", "--query-timeout" }, description = "Timeout in milliseconds")
//    public Long queryTimeoutInMs = null;
//
//    @Option(names = { "-y", "--delay" }, description = "Delay between query requests in milliseconds")
//    public Long delayInMs = 0l;
//
//    @Option(names = { "-x", "--experiment" },
//            description = "IRI for the experiment. Configuration and start/end time time will be attached to it.")
//    public String experimentIri = null;


    /**
     * A file containing a list of queries. Will be created from the log file if given.
     */
//	    @Parameter
//	    private int warmUpCount;
//	    private int runCount;


//	    @Parameter(defaultValue = "${project.build.directory}/tdb2")
//	    private File outputFolder;

    /** Output file (the folder as an archive) */
    @Parameter(defaultValue = "${project.build.directory}/lsq.trig")
    private File outputFile;

//	    public static class FileToGraphMapping {
//	        protected File file;
//	        protected String graph;
//
//	        public File getFile() { return file; }
//	        public void setFile(File file) { this.file = file; }
//	        public String getGraph() { return graph; }
//	        public void setGraph(String graph) { this.graph = graph; }
//	    }

    /** Mapping of extra files to graphs. */
//	    @Parameter
//	    private List<FileToGraphMapping> files = new ArrayList<>();

//	    /** Whether to create an archive from the database folder */
//	    @Parameter(defaultValue = "true")
//	    private boolean createArchive;
//
//	    /** Whether to attach the created archive (only applicable if an archive was created) */
//	    @Parameter(defaultValue = "true")
//	    private boolean attachArchive;

    /** Output format */
//	    @Parameter
//	    private String outputFormat;

    @Override
    public void execute() throws MojoExecutionException {
        if (!skip) {
        	JenaMojoHelper.execJenaBasedMojo(this::executeActual);
        }
    }

    public void executeActual() throws Exception {
        Log logger = getLog();

//        Node metaGraphNode = metaGraph != null && !metaGraph.isBlank()
//            ? (metaGraph.equalsIgnoreCase("default")
//                    ? Quad.defaultGraphIRI
//                    : NodeFactory.createURI(metaGraph))
//            : null;

        initResultTdbStore();

        CmdLsqRdfizeBase rdfizeCmd = new CmdLsqRdfizeBase();
        // rdfizeCmd.nonOptionArgs = analyzeCmd.nonOptionArgs;
        rdfizeCmd.noMerge = true;
        rdfizeCmd.inputLogFormat = logFormat;
        rdfizeCmd.nonOptionArgs.add(logFile.getAbsolutePath());

        // Do not emit remote executions.
        // Note: the endpoint url becomes part of the IRIs of remote executions.
        rdfizeCmd.rdfizationLevel.queryOnly = true;

        String baseIri = rdfizeCmd.baseIri;

        String datasetLabel = UriToPathUtils.resolvePath(datasetId).toString()
                .replace('/', '-');

        String expId = MainCliLsq.createExperimentId(datasetId);

        Model expConfigModel = ModelFactory.createDefaultModel();

        RdfDataRefSparqlEndpoint endpoint = expConfigModel.createResource().as(RdfDataRefSparqlEndpoint.class)
                .setServiceUrl(serviceUrl)
                ;

        ExperimentConfig expConfigRaw = expConfigModel.createResource().as(ExperimentConfig.class)
                .setIdentifier(expId)
                .setBaseIri(baseIri)
                .setDataRef(endpoint)
                .setDatasetLabel(datasetLabel)
                ;

        ExperimentConfig expConfig = SkolemizeBackport.skolemize(expConfigRaw, baseIri, ExperimentConfig.class, null);


        XSDDateTime expExecTimestamp = new XSDDateTime(GregorianCalendar.from(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)));

        Model expExecModel = ModelFactory.createDefaultModel();
        ExperimentExec expExecRaw = expExecModel.createResource().as(ExperimentExec.class)
                .setConfig(expConfig)
                .setTimestamp(expExecTimestamp);

        // ExperimentConfig cfg = Skolemize.skolemize(rawCfg, baseIri, ExperimentConfig.class, null);
        ExperimentExec expExec = SkolemizeBackport.skolemize(expExecRaw, expConfigModel, baseIri, ExperimentExec.class, null);

        String tdbPath = resultTdbStore.getCanonicalFile().getAbsolutePath();
        logger.info("TDB2 benchmark db location: " + tdbPath);
        Dataset resultDataset = TDB2Factory.connectDataset(tdbPath);

        LsqEnricherShell enricherFactory = new LsqEnricherShell(baseIri, enrichers, LsqEnricherRegistry::get);
        Function<Resource, Resource> enricher = enricherFactory.get();

        Path outputPath = outputFile.toPath();

        FileNameParser fileNameParser = FileNameParser.of(
                x -> ContentTypeUtils.getCtExtensions().getAlternatives().containsKey(x.toLowerCase()),
                x -> ContentTypeUtils.getCodingExtensions().getAlternatives().containsKey(x.toLowerCase()));

        String fileName = outputPath.getFileName().toString();

        FileName fileInfo = fileNameParser.parse(fileName);
        Function<OutputStream, OutputStream> encoder = RDFDataMgrExBackport.encoder(fileInfo.getEncodingParts());



        String salt = MainCliLsq.getOrCreateSalt(rdfizeCmd);

        FileUtilsBackport.safeCreate(outputPath, encoder, OverwriteMode.SKIP, outStream -> {
            StreamRDF out = StreamRDFWriter.getWriterStream(outStream, RDFFormat.TRIG_BLOCKS);
            out.start();
            PrefixMapping pmap = MainCliLsq.addLsqPrefixes(new PrefixMappingImpl());
            StreamRDFOps.sendPrefixesToStream(pmap, out);

            // StreamRDF writer = StreamRDFWriter.getWriterStream(out, RDFFormat.TRIG_BLOCKS);

            try (RDFConnection indexConn = RDFConnection.connect(resultDataset)) {
                // Emit the config
                // XXX Also write to the tdb dataset?
                // if (metaGraphNode != null) {
                    //Dataset metaDs = new DatasetOneNgImpl(DatasetGraphOneNgImpl.create(metaGraphNode, expConfigModel.getGraph()));
                StreamRDFOps.sendDatasetToStream(DatasetOneNgImplBackport.naturalDataset(expConfig).asDatasetGraph(), out);
                StreamRDFOps.sendDatasetToStream(DatasetOneNgImplBackport.naturalDataset(expExec).asDatasetGraph(), out);
                // RDFDataMgr.write(out, DatasetOneNgImpl.naturalDataset(expExec), RDFFormat.TRIG_BLOCKS);
                // }

                RdfDataRefSparqlEndpoint dataRef = expConfig.getDataRef(); // expExec.getConfig().getDataRef();
                try (RdfDataPod dataPod = DataPods.fromDataRef(dataRef)) {

                    // Warmup runs have negative ids; run 0 is the first non-warmup run
                    int warmUpOffset = -Math.max(warmupRuns, 0);
                    for (int i = warmUpOffset; i < runs; ++i) {
                        XSDDateTime expRunTimestamp = new XSDDateTime(GregorianCalendar.from(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)));

                        Model runModel = ModelFactory.createDefaultModel();
                        ExperimentRun expRunRaw = runModel.createResource().as(ExperimentRun.class)
                                // .setConfig(cfg)
                                .setExec(expExec)
                                .setRunId(i)
                                .setTimestamp(expRunTimestamp)
                                ;

                        ExperimentRun expRun = SkolemizeBackport.skolemize(expRunRaw, ModelUtilsBackport.union(expConfigModel, expExecModel), baseIri, ExperimentRun.class, null);

                        boolean isWarmupRun = i < 0;

                        Flowable<LsqQuery> queryFlow = MainCliLsq.createLsqRdfFlow(rdfizeCmd, salt)
//                        		.map(r -> {
//                        			Resource x = ModelFactory.createDefaultModel().createResource();
//                        			 x.getModel().add(r.getModel());
//                        			 return x;
//                        		})
                                .map(r -> r.as(LsqQuery.class));

                        if (isWarmupRun && (warmupTaskLimit != null && warmupTaskLimit >= 0)) {
                            queryFlow = queryFlow.take(warmupTaskLimit);
                        }

                        // Create a new model for the run - but also add the config model
//                        Model runModel = ModelFactory.createDefaultModel();
//                        runModel.add(cfgModel);
//                        ExperimentRun runCfgRaw = runModel.createResource().as(ExperimentRun.class)
//                                // .setConfig(cfg)
//                                .setExec(expExec)
//                                .setRunId(i)
//                                .setTimestamp(timestamp);

                        // System.out.println("cfg: " + runCfg.getConfig());

                        // if (metaGraphNode != null) {
//                            Model runCfgOnly = ModelFactory.createDefaultModel();
//                            runCfgOnly.add(runModel);
//                            runCfgOnly.remove(expConfigModel);

                            // ExperimentConfig cfg = Skolemize.skolemize(rawCfg, baseIri, ExperimentConfig.class, null).as(ExperimentConfig.class);
                            // Dataset metaDs = new DatasetOneNgImpl(DatasetGraphOneNgImpl.create(metaGraphNode, runCfgOnly.getGraph()));
                        if (!isWarmupRun || warmupLog) {
                            StreamRDFOps.sendDatasetToStream(DatasetOneNgImplBackport.naturalDataset(expRun).asDatasetGraph(), out);
                        }

                        try (SparqlQueryConnection benchmarkConn =
                                SparqlQueryConnectionWithReconnect.create(() -> dataPod.getConnection())) {
                            StreamRDF effectiveOut = !isWarmupRun || warmupLog
                                    ? out
                                    : StreamRDFLib.sinkNull();
                            LsqBenchmarkProcessor.process(effectiveOut, queryFlow, baseIri, expConfig, expExec, expRun, enricher, benchmarkConn, indexConn);
                        }
                    }
                }
            } finally {
                try {
                    out.finish();
                } finally {
                    resultDataset.close();
                    TDBInternal.expel(resultDataset.asDatasetGraph());
                }
            }
        });


    }
//
////            MainCliLsq.benchmarkExecute();
////        }
//
//
//
////        ExperimentRun run = tryLoadRun(configSrc)
////                .orElseThrow(() -> new IllegalArgumentException(
////                        "Could not detect a resource with " + LSQ.Terms.config + " property in " + configSrc));
//
////        ExperimentConfig cfg = run.getConfig();
//
//
//        // String baseIri = rdfizeCmd.baseIri;
//        // TODO How to obtain the baseIRI? A simple hack would be to 'grep'
//        // for the id part before lsqQuery
//        // The only 'clean' option would be to make the baseIri an attribute of every lsqQuery resource
//        // which might be somewhat overkill
//
//        LsqEnricherShell enricherFactory = new LsqEnricherShell(baseIri, enrichers, LsqEnricherRegistry::get);
//        Function<Resource, Resource> enricher = enricherFactory.get();
//
//        Flowable<ResourceInDataset> flow = MainCliLsq.createLsqRdfFlow(rdfizeCmd);
//
//        Flowable<Dataset> dsFlow = flow.map(rid -> {
//            // TODO The enricher may in general rename the input resource due to skolemization - handle this case
//            enricher.apply(rid);
//            return rid;
//        })
////	        .map(ResourceInDatasetImpl::createFromCopyIntoResourceGraph)
//        .map(ResourceInDataset::getDataset);
//
//        Path outputPath = outputFile.toPath();
//        FileUtils.safeCreate(outputPath, OverwriteMode.SKIP, out -> {
//            RDFDataMgrRx.writeDatasets(dsFlow, out, RDFFormat.TRIG_BLOCKS);
//        });
//    }
}
