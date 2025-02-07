package org.aksw.maven.plugin.mvnize.util;

import java.util.Optional;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class Xpp3DomUtils {
    public static Xpp3Dom addEntryAsChild(Xpp3Dom parent, String key, String value) {
        if (value != null) {
            Xpp3Dom child = new Xpp3Dom(key);
            child.setValue(value);
            parent.addChild(child);
        }
        return parent;
    }

    public static String getChildAsString(Xpp3Dom node, String childName) {
        String result = Optional.ofNullable(node)
            .map(n -> n.getChild(childName))
            .map(Xpp3Dom::getValue)
            .orElse(null);
        return result;
    }

    /**
     *
     * @param node The node from which to take classifier and type.
     * @param prototype An artifact from which to take groupId, artifact and version.
     * @return
     */
    public static Artifact extractArtifact(Xpp3Dom node, Artifact prototype) {
        String classifier = getChildAsString(node, "classifier");
        String type = getChildAsString(node, "type");
        return new DefaultArtifact(
            prototype.getGroupId(), prototype.getArtifactId(), prototype.getVersion(),
            prototype.getScope(), type, classifier, new DefaultArtifactHandler(type));
    }
}
