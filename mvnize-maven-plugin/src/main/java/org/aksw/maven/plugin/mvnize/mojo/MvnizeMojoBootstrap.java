package org.aksw.maven.plugin.mvnize.mojo;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.aksw.maven.plugin.mvnize.util.BuildHelperUtils;
import org.aksw.maven.plugin.mvnize.util.PomUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.Plugin;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * Based on a pom.xml generated by mvn mvnize:generate-pom,
 * generate a bootstrap-pom.xml file which adds all
 * artifacts to its dependencies section.
 */
@Mojo(name = "bootstrap", requiresProject = true)
public class MvnizeMojoBootstrap extends AbstractMojo {
    @Parameter(defaultValue = "${project.file}", readonly = true)
    private File sourcePomFile;

    @Parameter(property = "parentId", required = false)
    private String parentId;

    @Parameter(property = "artifactId", required = false, defaultValue = "my-group:my-artifact:0.0.1-SNAPSHOT")
    private String artifactId;

    /** The output file. */
    @Parameter(property = "output", required = false, defaultValue = "generated.pom.xml")
    private File targetPomFile;

    /** The Maven project */
    @Parameter(defaultValue = "${project}", readonly = false, required = false)
    private MavenProject project;

    @Override
    public void execute() throws MojoExecutionException {
        if (targetPomFile.equals(sourcePomFile)) {
            throw new RuntimeException("Target pom file must not be the project's pom!");
        }

        // Artifact of the source pom from which to the dependencies
        Artifact targetArtifact = PomUtils.parseArtifact(artifactId);

        // Use the given options to build the target artifact
        // Use defaults if omitted
        Artifact sourceArtifact = new DefaultArtifact(
            project.getGroupId(), project.getArtifactId(), project.getVersion(), null, project.getPackaging(), null, new DefaultArtifactHandler(project.getPackaging()));

        Model targetModel;
        if (targetPomFile.exists()) {
            targetModel = PomUtils.loadExistingPom(targetPomFile);
        } else {
            targetModel = PomUtils.createNewPom(targetArtifact);
        }

        BuildHelperUtils.addArtifact(targetModel, targetArtifact);

        if (parentId != null) {
            Artifact parent = PomUtils.parseArtifact(parentId);
            PomUtils.setParent(targetModel, parent);
        }

        Set<String> existingDepIds = targetModel.getDependencies().stream()
            .map(PomUtils::toArtifact)
            .map(PomUtils::toId)
            .collect(Collectors.toSet());

        Plugin buildHelperPlugin = BuildHelperUtils.getPlugin(project.getBuild());
        List<Artifact> sourceAttachments = BuildHelperUtils.listArtifacts(buildHelperPlugin, sourceArtifact);
        List<Dependency> dependencies = sourceAttachments.stream()
            .filter(artifact -> !existingDepIds.contains(PomUtils.toId(artifact)))
            .map(PomUtils::toDependency)
            .toList();

        targetModel.getDependencies().addAll(dependencies);

        PomUtils.writePomFile(targetPomFile, targetModel);
    }
}
