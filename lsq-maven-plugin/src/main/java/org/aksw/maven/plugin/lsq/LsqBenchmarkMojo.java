package org.aksw.maven.plugin.lsq;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.function.Function;

import org.aksw.commons.io.util.FileUtils;
import org.aksw.commons.io.util.FileUtils.OverwriteMode;
import org.aksw.commons.io.util.UriToPathUtils;
import org.aksw.jena_sparql_api.conjure.datapod.api.RdfDataPod;
import org.aksw.jena_sparql_api.conjure.datapod.impl.DataPods;
import org.aksw.jena_sparql_api.conjure.dataref.rdf.api.RdfDataRefSparqlEndpoint;
import org.aksw.jenax.dataaccess.sparql.connection.reconnect.SparqlQueryConnectionWithReconnect;
import org.aksw.simba.lsq.cli.cmd.base.CmdLsqRdfizeBase;
import org.aksw.simba.lsq.cli.main.MainCliLsq;
import org.aksw.simba.lsq.enricher.benchmark.core.LsqBenchmarkProcessor;
import org.aksw.simba.lsq.enricher.core.LsqEnricherRegistry;
import org.aksw.simba.lsq.enricher.core.LsqEnricherShell;
import org.aksw.simba.lsq.model.ExperimentConfig;
import org.aksw.simba.lsq.model.ExperimentRun;
import org.aksw.simba.lsq.model.LsqQuery;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.SparqlQueryConnection;
import org.apache.jena.tdb2.TDB2Factory;
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
    private int runCount;

    @Parameter(defaultValue = "SELECT * { ?s a <http://www.example.org/Thing> }")
    protected String testQuery;

    @Parameter(property = "lsq.skip", defaultValue = "false")
    protected boolean skip;

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
        try {
            if (!skip) {
                doExecute();
            }
        } catch (Exception e) {
            throw new MojoExecutionException(e);
        }
    }

    public void doExecute() throws Exception {
        Log logger = getLog();

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

        Model model = ModelFactory.createDefaultModel();

        RdfDataRefSparqlEndpoint endpoint = model.createResource().as(RdfDataRefSparqlEndpoint.class)
                .setServiceUrl(serviceUrl)
                ;

        ExperimentConfig cfg = model.createResource().as(ExperimentConfig.class)
                .setIdentifier(expId)
                .setBaseIri(baseIri)
                .setDataRef(endpoint)
                .setDatasetLabel(datasetLabel)
                ;

        String tdbPath = resultTdbStore.getCanonicalFile().getAbsolutePath();
        logger.info("TDB2 benchmark db location: " + tdbPath);
        Dataset resultDataset = TDB2Factory.connectDataset(tdbPath);

        LsqEnricherShell enricherFactory = new LsqEnricherShell(baseIri, enrichers, LsqEnricherRegistry::get);
        Function<Resource, Resource> enricher = enricherFactory.get();

        XSDDateTime timestamp = new XSDDateTime(GregorianCalendar.from(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)));
        Path outputPath = outputFile.toPath();

        FileUtils.safeCreate(outputPath, OverwriteMode.SKIP, out -> {

            try (RDFConnection indexConn = RDFConnection.connect(resultDataset)) {
                RdfDataRefSparqlEndpoint dataRef = cfg.getDataRef();
                try (RdfDataPod dataPod = DataPods.fromDataRef(dataRef)) {
                    for (int i = 0; i < runCount; ++i) {

                        Flowable<LsqQuery> queryFlow = MainCliLsq.createLsqRdfFlow(rdfizeCmd)
//                        		.map(r -> {
//                        			Resource x = ModelFactory.createDefaultModel().createResource();
//                        			 x.getModel().add(r.getModel());
//                        			 return x;
//                        		})
                                .map(r -> r.as(LsqQuery.class));


                        // Model model = ModelFactory.createDefaultModel();
                        ExperimentRun runCfg = model.createResource().as(ExperimentRun.class)
                                .setConfig(cfg)
                                .setIdentifier(baseIri)
                                .setRunId(i)
                                .setTimestamp(timestamp);

                                try (SparqlQueryConnection benchmarkConn =
                                        SparqlQueryConnectionWithReconnect.create(() -> dataPod.getConnection())) {
                                    LsqBenchmarkProcessor.process(out, queryFlow, baseIri, cfg, runCfg, enricher, benchmarkConn, indexConn);
                                } finally {
                                    resultDataset.close();
                                }
                        }
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
