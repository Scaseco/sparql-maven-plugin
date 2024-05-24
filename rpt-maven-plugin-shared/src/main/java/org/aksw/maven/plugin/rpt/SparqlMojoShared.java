package org.aksw.maven.plugin.rpt;

import java.io.File;
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
    protected String classifier;

    /** Type under which to attach the output if 'attach' is true */
    @Parameter
    protected String type;

    @Component
    private MavenProjectHelper mavenProjectHelper;
    
    @Override
    public void execute() throws MojoExecutionException {
        try {
            CmdSparqlIntegrateMain cmd = new CmdSparqlIntegrateMain();
            cmd.nonOptionArgs = args;
            if (outputFile != null) {
            	String outFileStr = outputFile.getAbsolutePath();
                cmd.outputSpec = new OutputSpec();
                cmd.outputSpec.outFile = outFileStr;
            }
            cmd.engine = engine;
            cmd.outFormat = outputFormat;
            cmd.env = env;
            cmd.debugMode = true;

            cmd.call();
            
            if (attach) {
                String t = type != null ? type : "trig";
                mavenProjectHelper.attachArtifact(project, t, classifier, outputFile);
            }

//
//            // create the artifact to search for
//            Artifact artifact = new DefaultArtifact(groupId, artifactId, project.getPackaging(), "[" + startingVersion + ",)");
//            // create the version request object
//            VersionRangeRequest rangeRequest = new VersionRangeRequest();
//            rangeRequest.setArtifact(artifact);
//            rangeRequest.setRepositories(projectRepos);
//            // search for the versions
//            VersionRangeResult rangeResult = repoSystem.resolveVersionRange(repoSession, rangeRequest);
//            getLog().info("Retrieving version of " + artifact.getGroupId() + ":" + artifact.getArtifactId() + ":" + artifact.getExtension());
//            List<Version> availableVersions = rangeResult.getVersions();
//            getLog().info("Available versions " + availableVersions);
//            // if we don't want the snapshots, filter them
//            if (!includeSnapshots) {
//                filterSnapshots(availableVersions);
//            }
//            // order the version from the newer to the older
//            Collections.reverse(availableVersions);
//            ArrayList<String> versionList = new ArrayList<>();
//            availableVersions.forEach((version) -> {
//                versionList.add(version.toString());
//            });
//            // set the poject property
//            project.getProperties().put(versionListPropertyName, versionList);
        } catch (Exception ex) {
            throw new MojoExecutionException("Error in plugin", ex); //ex.getCause());
        }
    }

//    private void filterSnapshots(List<Version> versions) {
//        for (Iterator<Version> versionIterator = versions.iterator(); versionIterator.hasNext();) {
//            Version version = versionIterator.next();
//            // if the version is a snapshot, get rid of it
//            if (version.toString().endsWith("SNAPSHOT")) {
//                versionIterator.remove();
//            }
//        }
//    }
}
