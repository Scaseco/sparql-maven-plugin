package org.aksw.maven.plugin.mvnize;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.model.Build;
import org.apache.maven.model.Model;
import org.apache.maven.model.Parent;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.model.io.xpp3.MavenXpp3Writer;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

@Mojo(name = "generate-pom", requiresProject = false)
public class MvnizeMojo extends AbstractMojo {
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
        Artifact artifact = parseArtifact(artifactId);
        artifact.setFile(file);

        if (pomFile == null) {
            pomFile = new File("pom.xml");
        }

        Model model;
        if (pomFile.exists()) {
            model = loadExistingPom(pomFile);
        } else {
            model = createNewPom(artifact);
        }

        if (parentId != null) {
            Artifact parent = parseArtifact(parentId);
            Parent p = new Parent();
            p.setGroupId(parent.getGroupId());
            p.setVersion(parent.getVersion());
            p.setArtifactId(parent.getArtifactId());
            model.setParent(p);
        }

        addArtifact(model, artifact);

        writePomFile(pomFile, model);
    }

    private static Model loadExistingPom(File pomFile) throws MojoExecutionException{
        try (FileReader reader = new FileReader(pomFile)) {
            return new MavenXpp3Reader().read(reader);
        } catch (IOException | XmlPullParserException e) {
            throw new MojoExecutionException("Error reading existing pom.xml", e);
        }
    }

    private static Model createNewPom(Artifact artifact) {
        Model model = new Model();
        model.setModelVersion("4.0.0");
        model.setGroupId(artifact.getGroupId());
        model.setArtifactId(artifact.getArtifactId());
        model.setVersion(artifact.getVersion());
        model.setPackaging("pom");
        // model.setDescription(description);

        Properties properties = model.getProperties();
        if (!properties.contains("build-helper-maven-plugin.version")) {
            properties.setProperty("build-helper-maven-plugin.version", "3.3.0");
        }

        return model;
    }

    private void writePomFile(File pomFile, Model model) throws MojoExecutionException {
        try (FileWriter writer = new FileWriter(pomFile)) {
            new MavenXpp3Writer().write(writer, model);
            getLog().info("pom.xml has been generated/updated successfully.");
        } catch (IOException e) {
            throw new MojoExecutionException("Error writing pom.xml", e);
        }
    }


    public static void addArtifact(Model model, Artifact artifact) {
        File file = artifact.getFile();
        if (file != null) {
            Build build = model.getBuild();
            if (build == null) {
                build = new Build();
                model.setBuild(build);
            }

            Plugin plugin = BuildHelperUtils.getOrCreatePlugin(build);
            BuildHelperUtils.attachArtifact(plugin, file.toString(), artifact.getType(), artifact.getClassifier());
        }
    }

    public static Artifact parseArtifact(String coordinates) {
        String[] parts = coordinates.split(":");
        String groupId = parts.length > 0 ? parts[0] : null;
        String artifactId = parts.length > 1 ? parts[1] : null;
        String version = parts.length > 2 ? parts[2] : null;
        String type = parts.length > 3 ? parts[3] : "jar"; // Default type is "jar"
        String classifier = parts.length > 4 ? parts[4] : null;

        return new DefaultArtifact(groupId, artifactId, version, null, type, classifier, new DefaultArtifactHandler(type));
    }
}
