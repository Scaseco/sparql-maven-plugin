package org.aksw.maven.plugin.sparql;

import java.util.List;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.RemoteRepository;

/**
 * Goal which generate a version list.
 *
 */
@Mojo(name = "integrate")
public class SparqlMojo extends AbstractMojo {

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

    /** The SPARQL engine to use for processing */
    @Parameter(defaultValue = "mem")
    private String engine;

    /** The SPARQL engine to use for processing */
    @Parameter
    private String tmpdir;

    /** Properties for use in substitution*/
    @Parameter
    private Map<String, String> env;

    enum SparqlStmtType {

    }

    public static class Arg {
        protected String query;
        protected String update;
        protected String load;

        public String getQuery() {
            return query;
        }
        public void setQuery(String query) {
            this.query = query;
        }
        public String getUpdate() {
            return update;
        }
        public void setUpdate(String update) {
            this.update = update;
        }
        public String getLoad() {
            return load;
        }
        public void setLoad(String load) {
            this.load = load;
        }

        public QueryType getQueryType() {
            return null;
        }
    }

    /** Arguments of the SPARQL processor */
    @Parameter
    private List<Arg> args;

    /** Output file */
    @Parameter
    private String outputFile;

    /** Output format */
    @Parameter
    private String outputFormat;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            doExecute();
        } catch (Exception e) {
            throw new MojoExecutionException(e);
        }
    }

    public void doExecute() throws Exception {
        DatasetGraph dsg = DatasetGraphFactory.create();
        for (Arg arg : args) {
            String value;

            value = arg.getQuery();
            Query query = QueryFactory.create(value);

            QueryExec.newBuilder().dataset(dsg).query(query);
        }
    }
}
