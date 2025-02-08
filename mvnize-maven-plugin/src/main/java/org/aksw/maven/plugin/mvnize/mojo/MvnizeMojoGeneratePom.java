package org.aksw.maven.plugin.mvnize.mojo;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Optional;

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

    @Parameter(property = "ignoreMissingFile", required = false, defaultValue = "false")
    private boolean ignoreMissingFile;

    @Override
    public void execute() throws MojoExecutionException {
        Artifact artifact = PomUtils.parseArtifact(artifactId);

        File absFile = file.getAbsoluteFile();
        if (!ignoreMissingFile && !absFile.exists()) {
            throw new MojoExecutionException(new FileNotFoundException("File not found: " + absFile));
        }

        File projectFile = Optional.of(project)
            .map(MavenProject::getFile)
            .map(File::getParentFile)
            .orElse(new File(""));
        Path projectAbsPath = projectFile.toPath().toAbsolutePath();

        Path fileAbsPath = absFile.toPath();
        Path fileRelPath = projectAbsPath.relativize(fileAbsPath);

        if (!fileAbsPath.startsWith(projectAbsPath)) {
            getLog().warn("File is located outside of the project directory: " + fileRelPath);
        }

        File relFile = fileRelPath.toFile();
        artifact.setFile(relFile);

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
