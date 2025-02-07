package org.aksw.maven.plugin.qlever;

import java.io.File;
import java.io.IOException;
import java.util.List;

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

import jenax.engine.qlever.docker.QleverConfRun;
import jenax.engine.qlever.docker.QleverRunner;

@Mojo(name = "run", defaultPhase = LifecyclePhase.PACKAGE)
public class QleverRunMojo extends AbstractMojo {

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

    // @Parameter(property = "qlever.skip", defaultValue = "false")
    // protected boolean skip;

    /**
     * Comma separated list of dependency type suffixes which to include.
     * A type matches if the string after stripping the suffix is empty or ends with a dot.
     *
     * Examples:
     * "nt" matches the suffix "nt" because "" is the empty string.
     * "rml.ttl" matches the suffix "ttl" because "rml." ends with a dot.
     * "hint" does NOT match "nt" because "hi" is neither the empty string nor does it end with a dot.
     *
     */
    // TODO Generate content-type+encoding combinations from registries
    @Parameter(defaultValue = "nt,ttl,nq,trig,owl,nt.gz,ttl.gz,nq.gz,trig.gz,owl.gz,nt.bz2,ttl.bz2,nq.bz2,trig.bz2,owl.bz2")
    private String includeTypes;

    @Parameter(defaultValue = "${project.build.directory}/qlever")
    private File outputFolder;

    protected static final String NS = "qlever.";

    @Parameter(property = NS + "docker.image", defaultValue = "adfreiburg/qlever")
    protected String dockerImage;

    @Parameter(property = NS + "docker.tag", defaultValue = "commit-a307781")
    protected String dockerTag;

    @Parameter(property = NS + "indexBaseName", defaultValue = "${project.artifactId}-${project.version}")
    protected String indexBaseName;

    /** The port exposed the host. */
    @Parameter(property = NS + "port", defaultValue = "8080")
    protected Integer port; // hostPort

    /** The port in the container (qlever's -p option) - Usually there is no need to set this. */
    @Parameter(property = NS + "container.port", defaultValue = "8080")
    protected Integer containerPort;

    @Parameter(property = NS + "accessToken")
    protected String accessToken;

    @Parameter(property = NS + "numSimultaneousQueries")
    protected Integer numSimultaneousQueries;

    @Parameter(property = NS + "memoryMaxSize")
    protected String memoryMaxSize;

    @Parameter(property = NS + "cacheMaxSize")
    protected String cacheMaxSize;

    @Parameter(property = NS + "cacheMaxSizeSingleEntry")
    protected String cacheMaxSizeSingleEntry;

    @Parameter(property = NS + "lazyResultMaxCacheSize")
    protected String lazyResultMaxCacheSize;

    @Parameter(property = NS + "cacheMaxNumEntries")
    protected Long cacheMaxNumEntries;

    @Parameter(property = NS + "noPatterns")
    protected Boolean noPatterns;

    @Parameter(property = NS + "noPatternTrick")
    protected Boolean noPatternTrick;

    @Parameter(property = NS + "text")
    protected Boolean text;

    @Parameter(property = NS + "onlyPsoAndPosPermutations")
    protected Boolean onlyPsoAndPosPermutations;

    @Parameter(property = NS + "defaultQueryTimeout")
    protected String defaultQueryTimeout;

    @Parameter(property = NS + "serviceMaxValueRows")
    protected Long serviceMaxValueRows;

    @Parameter(property = NS + "throwOnUnboundVariables")
    protected Boolean throwOnUnboundVariables;

    protected QleverConfRun buildConf() {
        // String indexBaseName = project.getArtifactId() + "-" + project.getVersion();
        QleverConfRun result = new QleverConfRun();
        result.setIndexBaseName(indexBaseName);
        result.setPort(containerPort);
        result.setAccessToken(accessToken);
        result.setNumSimultaneousQueries(numSimultaneousQueries);
        result.setMemoryMaxSize(memoryMaxSize);
        result.setCacheMaxSize(cacheMaxSize);
        result.setCacheMaxSizeSingleEntry(cacheMaxSizeSingleEntry);
        result.setLazyResultMaxCacheSize(lazyResultMaxCacheSize);
        result.setCacheMaxNumEntries(cacheMaxNumEntries);
        result.setNoPatterns(noPatterns);
        result.setNoPatternTrick(noPatternTrick);
        result.setText(text);
        result.setOnlyPsoAndPosPermutations(onlyPsoAndPosPermutations);
        result.setDefaultQueryTimeout(defaultQueryTimeout);
        result.setServiceMaxValueRows(serviceMaxValueRows);
        result.setThrowOnUnboundVariables(throwOnUnboundVariables);
        return result;
    }

    @Override
    public void execute() throws MojoExecutionException {
        try {
            executeActual();
        } catch (Exception e) {
            throw new MojoExecutionException(e);
        }
    }

    protected void executeActual() throws NumberFormatException, IOException, InterruptedException {
        Log logger = getLog();
        String outputPath = outputFolder.toPath().toString();

        QleverConfRun conf = buildConf();
        QleverRunner.run(outputPath, dockerImage, dockerTag, port, conf);
    }
}
