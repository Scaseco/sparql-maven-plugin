package org.aksw.maven.plugin.mvnize.util;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.Parent;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.model.io.xpp3.MavenXpp3Writer;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

public class PomUtils {
    public static Model loadExistingPom(File pomFile) throws MojoExecutionException{
        try (FileReader reader = new FileReader(pomFile)) {
            return new MavenXpp3Reader().read(reader);
        } catch (IOException | XmlPullParserException e) {
            throw new MojoExecutionException("Error reading existing pom.xml", e);
        }
    }

    public static Model createNewPom(Artifact artifact) {
        Model model = new Model();
        model.setModelVersion("4.0.0");
        model.setGroupId(artifact.getGroupId());
        model.setArtifactId(artifact.getArtifactId());
        model.setVersion(artifact.getVersion());
        model.setPackaging("pom");
        // model.setDescription(description);
        return model;
    }

    public static void setParent(Model model, Artifact parent) {
        if (parent != null) {
            Parent p = toParent(parent);
            model.setParent(p);
        }
    }

    public static void writePomFile(File pomFile, Model model) throws MojoExecutionException {
        try (FileWriter writer = new FileWriter(pomFile)) {
            new MavenXpp3Writer().write(writer, model);
        } catch (IOException e) {
            throw new MojoExecutionException("Error writing pom.xml", e);
        }
    }

    public static String toId(Artifact artifact) {
        String result = List.of(
                artifact.getGroupId(),
                artifact.getArtifactId(),
                artifact.getVersion(),
                artifact.getType(),
                artifact.getClassifier())
            .stream()
            .collect(Collectors.joining(":"));
        return result;
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

    public static Parent toParent(Artifact artifact) {
        Parent result = new Parent();
        result.setGroupId(artifact.getGroupId());
        result.setVersion(artifact.getVersion());
        result.setArtifactId(artifact.getArtifactId());
        return result;
    }

    public static Dependency toDependency(Artifact artifact) {
        Dependency result = new Dependency();
        result.setGroupId(artifact.getGroupId());
        result.setArtifactId(artifact.getArtifactId());
        result.setVersion(artifact.getVersion());
        result.setClassifier(artifact.getClassifier()); // may be null
        result.setScope(artifact.getScope());
        result.setType(artifact.getType());
        // result.setOptional(artifact.isOptional());
        return result;
    }

    public static Artifact toArtifact(Dependency dep) {
        Artifact result = new DefaultArtifact(
            dep.getGroupId(), dep.getArtifactId(), dep.getVersion(),
            dep.getScope(), dep.getType(), dep.getClassifier(), new DefaultArtifactHandler(dep.getType()));
        return result;
    }
}
