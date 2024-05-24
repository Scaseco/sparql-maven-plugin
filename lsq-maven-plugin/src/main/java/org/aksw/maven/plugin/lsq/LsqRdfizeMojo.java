package org.aksw.maven.plugin.lsq;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.aksw.commons.io.util.FileUtils;
import org.aksw.commons.io.util.FileUtils.OverwriteMode;
import org.aksw.commons.util.derby.DerbyUtils;
import org.aksw.jenax.arq.dataset.api.ResourceInDataset;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrRx;
import org.aksw.simba.lsq.cli.cmd.base.CmdLsqRdfizeBase;
import org.aksw.simba.lsq.cli.main.MainCliLsq;
import org.aksw.simba.lsq.enricher.core.LsqEnricherRegistry;
import org.aksw.simba.lsq.enricher.core.LsqEnricherShell;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFFormat;
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

@Mojo(name = "rdfize", defaultPhase = LifecyclePhase.PACKAGE)
public class LsqRdfizeMojo extends AbstractMojo {

    static { DerbyUtils.disableDerbyLog(); }

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

    @Parameter(property = "lsq.skip", defaultValue = "false")
    protected boolean skip;

    @Parameter
    private File logFile;

    @Parameter
    private List<String> enrichers = new ArrayList<>();

    @Parameter
    private String logFormat;


    /**
     * A file containing a list of queries. Will be created from the log file if given.
     */
//    @Parameter
//    private int warmUpCount;
//    private int runCount;


//    @Parameter(defaultValue = "${project.build.directory}/tdb2")
//    private File outputFolder;

    /** Output file (the folder as an archive) */
    @Parameter(defaultValue = "${project.build.directory}/lsq.trig")
    private File outputFile;

    @Parameter(defaultValue = "true")
    protected boolean attach;

    @Parameter
    protected String classifier;

    @Parameter
    protected String type;

//    public static class FileToGraphMapping {
//        protected File file;
//        protected String graph;
//
//        public File getFile() { return file; }
//        public void setFile(File file) { this.file = file; }
//        public String getGraph() { return graph; }
//        public void setGraph(String graph) { this.graph = graph; }
//    }

    /** Mapping of extra files to graphs. */
//    @Parameter
//    private List<FileToGraphMapping> files = new ArrayList<>();

//    /** Whether to create an archive from the database folder */
//    @Parameter(defaultValue = "true")
//    private boolean createArchive;
//
//    /** Whether to attach the created archive (only applicable if an archive was created) */
//    @Parameter(defaultValue = "true")
//    private boolean attachArchive;

    /** Output format */
//    @Parameter
//    private String outputFormat;

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

        CmdLsqRdfizeBase rdfizeCmd = new CmdLsqRdfizeBase();
        // rdfizeCmd.nonOptionArgs = analyzeCmd.nonOptionArgs;
        rdfizeCmd.noMerge = true;
        rdfizeCmd.inputLogFormat = logFormat;
        rdfizeCmd.nonOptionArgs.add(logFile.getAbsolutePath());

        // Do not emit remote executions.
        // Note: the endpoint url becomes part of the IRIs of remote executions.
        rdfizeCmd.rdfizationLevel.queryOnly = true;

        String baseIri = rdfizeCmd.baseIri;
        // TODO How to obtain the baseIRI? A simple hack would be to 'grep'
        // for the id part before lsqQuery
        // The only 'clean' option would be to make the baseIri an attribute of every lsqQuery resource
        // which might be somewhat overkill

        LsqEnricherShell enricherFactory = new LsqEnricherShell(baseIri, enrichers, LsqEnricherRegistry::get);
        Function<Resource, Resource> enricher = enricherFactory.get();

        Flowable<ResourceInDataset> flow = MainCliLsq.createLsqRdfFlow(rdfizeCmd);

        Flowable<Dataset> dsFlow = flow.map(rid -> {
            // TODO The enricher may in general rename the input resource due to skolemization - handle this case
            enricher.apply(rid);
            return rid;
        })
//        .map(ResourceInDatasetImpl::createFromCopyIntoResourceGraph)
        .map(ResourceInDataset::getDataset);

        Path outputPath = outputFile.toPath();
        FileUtils.safeCreate(outputPath, OverwriteMode.SKIP, out -> {
            RDFDataMgrRx.writeDatasets(dsFlow, out, RDFFormat.TRIG_BLOCKS);
        });

        if (attach) {
            String t = type != null ? type : "trig";
            mavenProjectHelper.attachArtifact(project, t, classifier, outputFile);
        }
    }
}
