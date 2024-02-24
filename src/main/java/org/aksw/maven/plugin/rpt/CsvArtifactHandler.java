package org.aksw.maven.plugin.rpt;

import org.apache.maven.artifact.handler.ArtifactHandler;
import org.codehaus.plexus.component.annotations.Component;

// @Component(role = ArtifactHandler.class)
public class CsvArtifactHandler implements ArtifactHandler {

    @Override
    public String getExtension() {
        System.out.println("CsvArtifactHandler INVOKED! 1");
        return "csv";
    }

    @Override
    public String getDirectory() {
        System.out.println("CsvArtifactHandler INVOKED! 2");
        return "csvs";
    }

    @Override
    public String getClassifier() {
        System.out.println("CsvArtifactHandler INVOKED! 3");
        return "";
    }

    @Override
    public String getPackaging() {
        return "csv";
    }

    @Override
    public boolean isIncludesDependencies() {
        return true;
    }

    @Override
    public String getLanguage() {
        return "none";
    }

    @Override
    public boolean isAddedToClasspath() {
        return true;
    }
}
