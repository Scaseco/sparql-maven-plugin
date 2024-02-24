package org.aksw.maven.plugin.unnecessary;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.shared.dependency.graph.DependencyGraphBuilder;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

/** Not needed - see comments in {@link CsvArtifactHandler} */
// @Mojo(name = "copy-csv-dependencies", defaultPhase = LifecyclePhase.GENERATE_RESOURCES)
public class CopyCsvDependenciesMojo extends AbstractMojo {

    /** The repository system (Aether) which does most of the management. */
    @Component
    protected RepositorySystem repoSystem;

    /** The current repository/network configuration of Maven. */
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    protected RepositorySystemSession repoSession;

    /** The Maven project */
    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    protected MavenProject project;

    /** The project's remote repositories to use for the resolution of project dependencies. */
    @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true)
    protected List<RemoteRepository> projectRepos;

    @Parameter(defaultValue = "${project.build.directory}", required = true)
    private File outputDirectory;

    // https://stackoverflow.com/questions/52054692/initialize-full-dependency-tree-in-maven-aggregator-plugin
    // https://github.com/apache/maven-dependency-plugin/blob/master/pom.xml
    @Component(hint="default")
    private DependencyGraphBuilder dependencyGraphBuilder;


    public void execute() throws MojoExecutionException, MojoFailureException {
        DependencyFilter classpathFlter = DependencyFilterUtils.classpathFilter(
                org.eclipse.aether.util.artifact.JavaScopes.COMPILE);
        CollectRequest collectRequest = new CollectRequest();
        // collectRequest.setr
        // collectRequest.setRoot(new Dependency(project.getArtifact(), JavaScopes.RUNTIME));
        for (org.apache.maven.model.Dependency dep : project.getDependencies()) {
            if ("csv".equals(dep.getType())) {
                collectRequest.addDependency(new Dependency(new org.eclipse.aether.artifact.DefaultArtifact(
                        dep.getGroupId(), dep.getArtifactId(), dep.getClassifier(),
                        dep.getType(), dep.getVersion()), JavaScopes.COMPILE));
            }
        }
        collectRequest.setRepositories(project.getRemoteProjectRepositories());
        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFlter);

        try {
            DependencyResult dependencyResult = repoSystem.resolveDependencies(repoSession, dependencyRequest);
            for (ArtifactResult artifactResult : dependencyResult.getArtifactResults()) {
                File artifactFile = artifactResult.getArtifact().getFile();
                String filename = artifactFile.getName();
                Files.copy(artifactFile.toPath(), outputDirectory.toPath().resolve(filename));
            }
        } catch (DependencyResolutionException e) {
            getLog().error("Could not resolve dependencies", e);
        } catch (IOException e) {
            getLog().error("Could not resolve dependencies", e);
        }



//        // Logic to resolve and filter for CSV dependencies
//        // This is a placeholder; actual implementation will require working with Maven's APIs
//
//    	 ArtifactFilter artifactFilter = new IncludesArtifactFilter("groupId:artifactId:version");
//         ProjectBuildingRequest buildingRequest = new DefaultProjectBuildingRequest(session.getProjectBuildingRequest());
//         buildingRequest.setProject(project);
//        try{
//           DependencyNode depenGraphRootNode = dependencyGraphBuilder.buildDependencyGraph(buildingRequest, artifactFilter);
//           CollectingDependencyNodeVisitor visitor = new  CollectingDependencyNodeVisitor();
//           depenGraphRootNode.accept(visitor);
//           List<DependencyNode> children = visitor.getNodes();
//
//           getLog().info("CHILDREN ARE :");
//           for(DependencyNode node : children) {
//               Artifact atf = node.getArtifact();
//        }
//}catch(Exception e) {
//e.printStackTrace();
//}
//
//        List<Artifact> csvDependencies = project.getArtifacts().stream()
//                .filter(artifact -> "csv".equals(artifact.getType()))
//                .collect(Collectors.toList());
//
//        // Copy the resolved CSV files to the specified directory
//        for (Artifact csvArtifact : csvDependencies) {
//            File artifactFile = csvArtifact.getFile();
//            try {
//                Files.copy(artifactFile.toPath(), outputDirectory.toPath());
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    /** Resolve an artifact based on the provided configuration */
    public ArtifactResult resolveArtifact(Artifact artifact) throws ArtifactResolutionException {
        ArtifactRequest request = new ArtifactRequest();
        request.setArtifact(artifact);
        request.setRepositories(projectRepos);
        ArtifactResult result = repoSystem.resolveArtifact(repoSession, request);
        return result;
    }

}
