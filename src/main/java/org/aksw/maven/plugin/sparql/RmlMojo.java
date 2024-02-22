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
import org.eclipse.aether.repository.RemoteRepository;

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

        logger.info("OutputFile: " + outputFile);
        logger.info("OutputFormat: " + outputFormat);

        try {
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
}
