package org.aksw.maven.plugin.qlever;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
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

/**
 * Mojo to run a qlever database.
 *
 * <pre>
 * mvn qlever:run
 * </pre>
 *
 * For loading qlever database see {@link QleverMojoLoad}.
 */
@Mojo(name = "run", defaultPhase = LifecyclePhase.PACKAGE)
public class QleverMojoRun extends AbstractMojo {

    /** The repository system (Aether) which does most of the management. */
    @Component
    protected RepositorySystem repoSystem;

    /** The current repository/network configuration of Maven. */
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    protected RepositorySystemSession repoSession;

    /** The project's remote repositories to use for the resolution of project dependencies. */
    @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true)
    protected List<RemoteRepository> projectRepos;

    /** The Maven project */
    @Parameter(defaultValue = "${project}", readonly = true)
    protected MavenProject project;

    @Component
    protected MavenProjectHelper mavenProjectHelper;

    // @Parameter(property = "qlever.skip", defaultValue = "false")
    // protected boolean skip;

    @Parameter(defaultValue = "${project.build.directory}/qlever")
    protected File dbFolder;

    /** Prefix for qlever properties */
    protected static final String NS = "qlever.";

    @Parameter(property = NS + "docker.image", defaultValue = QleverConstants.dockerImage)
    protected String dockerImage;

    @Parameter(property = NS + "docker.tag", defaultValue = QleverConstants.dockerTag)
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
        String dbPathStr = dbFolder.toPath().toString();
        QleverConfRun conf = buildConf();
        QleverRunner.run(dbPathStr, dockerImage, dockerTag, port, conf);
    }
}
