package org.aksw.maven.plugin.unnecessary;

import org.apache.maven.artifact.handler.DefaultArtifactHandler;

/**
 * This class can be used to alias existing types such as "jar" with custom names such as "my-plugin".
 * This class does not appear to be suitable to copy files of specific types to directories.
 * (Even though there is a getDirectory() method).
 * One can use the maven-dependencies-plugin:copy-dependencies goal for this purpose.
 * Writing a custom plugin is not really worthwhile because the maven-dependencies-plugin is
 * exactly there to copy dependencies and it is very flexible.
 *
 * https://youtrack.jetbrains.com/issue/TW-56828/Maven-Snapshot-Dependency-Trigger-does-not-work-with-custom-ArtifactHandler
 */
// @Component(role = ArtifactHandler.class)
public class CsvArtifactHandler extends DefaultArtifactHandler {
    public CsvArtifactHandler() {
        super("csv");
        setAddedToClasspath(true);
        setIncludesDependencies(false);
        setExtension("csv");
        setLanguage("java");

        System.err.println("Loaded custom artifact handler: " + this.getClass().getName());
        System.out.println("Loaded custom artifact handler: " + this.getClass().getName());
    }
}

