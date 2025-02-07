package org.aksw.maven.plugin.mvnize.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.model.Build;
import org.apache.maven.model.Model;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.PluginExecution;
import org.codehaus.plexus.util.xml.Xpp3Dom;

public class BuildHelperUtils {
    public static final String GROUP_ID = "org.codehaus.mojo";
    public static final String ARTIFACT_ID = "build-helper-maven-plugin";

    public static final String EXECUTION_ID = "attach-artifacts";

    public static void addBuildHelperVersionProperty(Model model) {
        Properties properties = model.getProperties();
        if (!properties.contains("build-helper-maven-plugin.version")) {
            properties.setProperty("build-helper-maven-plugin.version", "3.3.0");
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
            attachArtifact(plugin, file.toString(), artifact.getType(), artifact.getClassifier());
        }
    }

    public static String getKey() {
        return createPlugin().getKey();
    }

    public static Plugin createPlugin() {
        Plugin plugin = new Plugin();
        plugin.setGroupId(GROUP_ID);
        plugin.setArtifactId(ARTIFACT_ID);
        plugin.setVersion("${build-helper-maven-plugin.version}"); // 3.3.0
        return plugin;
    }

//    public static PluginExecution addExecution(Plugin plugin) {
//        // Artifact artifact = new Artifact();
//        Xpp3Dom configuration = new Xpp3Dom("configuration");
//        Xpp3Dom artifacts = new Xpp3Dom("artifacts");
//
//        configuration.addChild(artifacts);
//        execution.setConfiguration(configuration);
//        plugin.addExecution(execution);
//
//        return plugin;
//    }

    public static Plugin getPlugin(Build build) {
        Map<String, Plugin> pluginsMap = build.getPluginsAsMap();
        Plugin plugin = pluginsMap.get(getKey());
        return plugin;
    }

    public static Plugin getOrCreatePlugin(Build build) {
        Plugin plugin = getPlugin(build);
        if (plugin == null) {
            plugin = createPlugin();
            build.addPlugin(plugin);
        }
        return plugin;
    }

    public static Xpp3Dom attachArtifact(Plugin plugin, String file, String type, String classifier) {
        Map<String, PluginExecution> executionsMap = plugin.getExecutionsAsMap();
        PluginExecution execution = executionsMap.get(EXECUTION_ID);
        if (execution == null) {
            execution = new PluginExecution();
            execution.setId(EXECUTION_ID);
            execution.setPhase("package");
            execution.addGoal("attach-artifact");
            plugin.addExecution(execution);
        }

        Xpp3Dom configuration = (Xpp3Dom)execution.getConfiguration();
        if (configuration == null) {
            configuration = new Xpp3Dom("configuration");
            execution.setConfiguration(configuration);
        }

        Xpp3Dom artifacts = configuration.getChild("artifacts");
        if (artifacts == null) {
            artifacts = new Xpp3Dom("artifacts");
            configuration.addChild(artifacts);
        }

        Xpp3Dom target = null;
        for (Xpp3Dom artifact : artifacts.getChildren()) {
            Xpp3Dom fileNode = artifact.getChild("file");
            String fileValue = fileNode.getValue();
            if (Objects.equals(fileValue, file)) {
                target = artifact;
                while (target.getChildCount() > 0) {
                    target.removeChild(0);
                }
                break;
            }
        }

        if (target == null) {
            target = new Xpp3Dom("artifact");
            artifacts.addChild(target);
        }

        Xpp3DomUtils.addEntryAsChild(target, "file", file);
        Xpp3DomUtils.addEntryAsChild(target, "type", type);
        Xpp3DomUtils.addEntryAsChild(target, "classifier", classifier);

        return target;
    }

    public static List<Artifact> listArtifacts(Plugin plugin, Artifact prototype) {
        Map<String, PluginExecution> executionsMap = plugin.getExecutionsAsMap();
        PluginExecution execution = executionsMap.get(EXECUTION_ID);
        List<Artifact> result = Optional.ofNullable(execution)
            .map(PluginExecution::getConfiguration)
            .map(conf -> (Xpp3Dom)conf)
            .map(conf -> conf.getChild("artifacts"))
            .stream()
            .flatMap(artifacts -> Arrays.asList(artifacts.getChildren()).stream())
            .map(node -> Xpp3DomUtils.extractArtifact(node, prototype))
            .collect(Collectors.toCollection(ArrayList::new));
        return result;
    }

//    public static Xpp3Dom attachArtifact(Plugin plugin, String file, String type, String classifier) {
//        PluginExecution execution = plugin.getExecutionsAsMap().get("attach-artifacts");
//
//        Xpp3Dom configuration = (Xpp3Dom)execution.getConfiguration(); // plugin.getConfiguration();
//        Xpp3Dom artifacts = configuration.getChild("artifacts");
//
//        Xpp3Dom artifact = new Xpp3Dom("artifact");
//        Xpp3DomUtils.addEntryAsChild(artifact, "file", file);
//        Xpp3DomUtils.addEntryAsChild(artifact, "type", type);
//        Xpp3DomUtils.addEntryAsChild(artifact, "classifier", classifier);
//
//        artifacts.addChild(artifact);
//
//        return artifact;
//    }
}

