package org.aksw.maven.plugin.mvnize.mojo;

import java.io.File;

import org.aksw.maven.plugin.mvnize.util.BuildHelperUtils;
import org.aksw.maven.plugin.mvnize.util.PomUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.model.Model;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "generate-pom", requiresProject = false)
public class MvnizeMojoGeneratePom extends AbstractMojo {
    @Parameter(defaultValue = "${project.file}", readonly = true)
    private File pomFile;

    @Parameter(property = "parentId", required = false)
    private String parentId;

    @Parameter(property = "artifactId", required = false)
    private String artifactId;

    @Parameter(property = "file", required = false)
    private File file;

    /** The Maven project */
    @Parameter(defaultValue = "${project}", readonly = false, required = false)
    private MavenProject project;

    @Override
    public void execute() throws MojoExecutionException {
        Artifact artifact = PomUtils.parseArtifact(artifactId);
        artifact.setFile(file);

        if (pomFile == null) {
            pomFile = new File("pom.xml");
        }

        Model model;
        if (pomFile.exists()) {
            model = PomUtils.loadExistingPom(pomFile);
        } else {
            model = PomUtils.createNewPom(artifact);
            BuildHelperUtils.addBuildHelperVersionProperty(model);
        }

        if (parentId != null) {
            Artifact parent = PomUtils.parseArtifact(parentId);
            PomUtils.setParent(model, parent);
        }

        BuildHelperUtils.addArtifact(model, artifact);

        PomUtils.writePomFile(pomFile, model);
        getLog().info("pom.xml has been generated/updated successfully.");
    }
}
