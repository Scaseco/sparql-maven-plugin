package org.aksw.maven.plugin.rpt;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.aksw.commons.util.derby.DerbyUtils;
import org.aksw.sparql_integrate.cli.cmd.CmdSparqlIntegrateMain;
import org.aksw.sparql_integrate.cli.cmd.CmdSparqlIntegrateMain.OutputSpec;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.RemoteRepository;

/**
 * Goal which generate a version list.
 *
 */
@Mojo(name = "integrate")
public class SparqlMojoShared extends AbstractMojo {

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

    /** The SPARQL engine to use for processing */
    @Parameter(defaultValue = "mem")
    private String engine;

    /** The SPARQL engine to use for processing */
    @Parameter
    private String tmpdir;

    /** Properties for use in substitution*/
    @Parameter
    private Map<String, String> env;

    /** Arguments of the SPARQL processor */
    @Parameter
    private List<String> args;

    /** Output file */
    @Parameter
    private File outputFile;

    @Parameter(defaultValue = "true")
    private boolean attach;

    /** Output format */
    @Parameter
    private String outputFormat;

    /** Classifier under which to attach the output if 'attach' is true */
    @Parameter
    protected String outputClassifier;

    /** Type under which to attach the output if 'attach' is true */
    @Parameter
    protected String outputType;

    /** May need to create the build directory if it does not exist */
    @Parameter(defaultValue = "${project.build.directory}", readonly = true)
    private File projectBuildDirectory;

    @Parameter(property = "rpt.skip", defaultValue = "false")
    protected boolean skip;

    @Component
    private MavenProjectHelper mavenProjectHelper;
        
    @Override
    public void execute() throws MojoExecutionException {
    	if (!skip) {
    		JenaMojoHelper.execJenaBasedMojo(this::executeActual);
    	}
    }

    protected void executeActual() throws Exception {
        CmdSparqlIntegrateMain cmd = new CmdSparqlIntegrateMain();
        cmd.nonOptionArgs = args;
        if (outputFile != null) {
        	Path outputPath = outputFile.toPath().toAbsolutePath();
        	Path outputParentDir = outputPath.getParent();
        	if (outputParentDir != null) {
        		Files.createDirectories(outputParentDir);
        	}
        	
        	String outFileStr = outputPath.toString();
            cmd.outputSpec = new OutputSpec();
            cmd.outputSpec.outFile = outFileStr;
        }
        cmd.engine = engine;
        cmd.outFormat = outputFormat;
        cmd.env = env;
        cmd.debugMode = true;

        // TODO If outputFormat is not set then try to derive it from outputType 
        
        cmd.call();
        
        if (attach) {
        	// TODO Use SparqlSciptProcessor (or its utils) to determine an absent output format
        	// from arguments
            String t = outputType != null ? outputType : "trig";
            mavenProjectHelper.attachArtifact(project, t, outputClassifier, outputFile);
        }
    }
}
