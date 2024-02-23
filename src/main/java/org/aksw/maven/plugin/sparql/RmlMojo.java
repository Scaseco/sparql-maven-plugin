package org.aksw.maven.plugin.sparql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.aksw.commons.util.derby.DerbyUtils;
import org.aksw.rml.jena.impl.RmlToSparqlRewriteBuilder;
import org.aksw.sparql_integrate.cli.cmd.CmdSparqlIntegrateMain;
import org.aksw.sparql_integrate.cli.cmd.CmdSparqlIntegrateMain.OutputSpec;
import org.apache.jena.query.Query;
import org.apache.maven.model.Dependency;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.sis.system.Shutdown;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;

@Mojo(name = "rml") // TODO Consider rename to "integrate" in order to align with rdf-processing-toolkit
public class RmlMojo extends AbstractMojo {

    static { DerbyUtils.disableDerbyLog(); }

    /** The repository system (Aether) which does most of the management. */
    @Component
    protected RepositorySystem repoSystem;

    /** The current repository/network configuration of Maven. */
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    protected RepositorySystemSession repoSession;

    /** The Maven project */
    @Parameter(defaultValue = "${project}", readonly = true)
    protected MavenProject project;

    /** The project's remote repositories to use for the resolution of project dependencies. */
    @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true)
    protected List<RemoteRepository> projectRepos;

    /** The RML execution engine */
    @Parameter(defaultValue = "jena", required = true)
    protected String engine;

    public static class Mapping {
        protected String type;
        protected String value;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }


    @Parameter
    protected List<Mapping> mappings = new ArrayList<>();

    /** Properties for use in substitution */
    @Parameter
    private Map<String, String> env;

    /** Arguments of the SPARQL processor */
    @Parameter
    private List<String> args;

    /** Output file */
    @Parameter
    private String outputFile;

    /** Output format */
    @Parameter
    private String outputFormat;

    @Override
    public void execute() throws MojoExecutionException {
        Log logger = getLog();

        try {
//            Artifact artifact = new DefaultArtifact("dcat.org.coypu.data.disasters", "disasters", "dcat", "ttl.bz2", "0.20240220.0016-1");
//            logger.info("################################");
//            resolveArtifact(artifact);
//    // 0.20240122.0609
//            logger.info("OutputFile: " + outputFile);
//            logger.info("OutputFormat: " + outputFormat);
//    project.getDependencies().add(toDependency(artifact, "compile"));


            RmlToSparqlRewriteBuilder builder = new RmlToSparqlRewriteBuilder();

            for (Mapping mapping : mappings) {
                String type = mapping.getType();
                String value = mapping.getValue();
                if ("file".equalsIgnoreCase(type)) {
                    builder.addRmlFile(value);
                } else if(type == null || "inline".equals(type) || type.isBlank()) {
                    logger.info("Adding RML String: " + value);
                    builder.addRmlString(value);
                } else {
                    throw new RuntimeException("Unknown mapping type: " + type);
                }
            }

            List<Entry<Query, String>> labeledQueries = builder.generate();

            logger.info("Generated " + labeledQueries.size() + " queries");

            // TODO Introduce a proper builder at sparql integrate!
            CmdSparqlIntegrateMain cmd = new CmdSparqlIntegrateMain();

            for (Entry<Query, String> entry : labeledQueries) {
                Query query = entry.getKey();

                if (logger.isDebugEnabled()) {
                    logger.debug("Enqueuing query: " + query);
                }

                cmd.nonOptionArgs.add(query.toString());
            }

            if (outputFile != null) {
                cmd.outputSpec = new OutputSpec();
                cmd.outputSpec.outFile = outputFile;
            }
            // cmd.engine = engine;
            cmd.outFormat = outputFormat;
            cmd.env = env;
            cmd.debugMode = true;

            cmd.call();

            // If the engine is sansa, then we need to run CmdSansaQuery rather than CmdRptIntegrate
            // There should be a common abstraction

        } catch (Exception ex) {
            throw new MojoExecutionException("Error in plugin", ex); //ex.getCause());
        } finally {
            try {
                Shutdown.stop((Class<?>)null);
            } catch (Exception e) {
                logger.error("Error during shutdown of Apache SIS", e);
            }
        }
    }

    /** Resolve an artifact based on the provided configuration */
    public ArtifactResult resolveArtifact(Artifact artifact) throws ArtifactResolutionException {
        ArtifactRequest request = new ArtifactRequest();
        request.setArtifact(artifact);
        request.setRepositories(projectRepos);
        ArtifactResult result = repoSystem.resolveArtifact(repoSession, request);
        return result;
    }

    /** Convert an aether artifact to a maven dependency */
    public static Dependency toDependency(Artifact artifact, String scope) {

        // Create a new Maven Dependency object
        Dependency dependency = new Dependency();

        // Set the groupId, artifactId, and version based on the Aether Artifact
        dependency.setGroupId(artifact.getGroupId());
        dependency.setArtifactId(artifact.getArtifactId());
        dependency.setVersion(artifact.getVersion());

        // Set the type if it's not the default 'jar'
        if (!"jar".equals(artifact.getExtension())) {
            dependency.setType(artifact.getExtension());
        }

        // Set the classifier if present
        if (artifact.getClassifier() != null && !artifact.getClassifier().isEmpty()) {
            dependency.setClassifier(artifact.getClassifier());
        }

        dependency.setScope(scope);

        return dependency;
    }
}
