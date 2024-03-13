package org.aksw.maven.plugin.jena;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.modify.request.UpdateLoad;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

@Mojo(name = "load", defaultPhase = LifecyclePhase.PACKAGE)
public class Tdb2MojoShared extends AbstractMojo {

    /** The repository system (Aether) which does most of the management. */
    @Component
    private RepositorySystem repoSystem;

    /** The current repository/network configuration of Maven. */
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    private RepositorySystemSession repoSession;

    /** The project's remote repositories to use for the resolution of project dependencies. */
    @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true)
    private List<RemoteRepository> projectRepos;

    /** The Maven project */
    @Parameter(defaultValue = "${project}", readonly = true)
    private MavenProject project;

    @Component
    private MavenProjectHelper mavenProjectHelper;

    /**
     * Comma separated list of dependency type suffixes which to include.
     * A type matches if the string after stripping the suffix is empty or ends with a dot.
     *
     * Examples:
     * "nt" matches the suffix "nt" because "" is the empty string.
     * "rml.ttl" matches the suffix "ttl" because "rml." ends with a dot.
     * "hint" does NOT match "nt" because "hi" is neither the empty string nor does it end with a dot.
     *
     */
    @Parameter(defaultValue = "nt,ttl,nq,trig,owl,nt.gz,ttl.gz,nq.gz,trig.gz,owl.gz,nt.bz2,ttl.bz2,nq.bz2,trig.bz2,owl.bz2")
    private String includeTypes;

    @Parameter(defaultValue = "${project.build.directory}/tdb2")
    private File outputFolder;

    /** Output file (the folder as an archive) */
    @Parameter(defaultValue = "${project.build.directory}/tdb2.tar.gz")
    private File outputFile;

    /** Output format */
//    @Parameter
//    private String outputFormat;

    @Override
    public void execute() throws MojoExecutionException {
        try {
            doExecute();
        } catch (Exception e) {
            throw new MojoExecutionException(e);
        }
    }

    public void doExecute() throws Exception {
        Log logger = getLog();

        // Test creation first before resolving dependencies
        Location location = Location.create(outputFolder.toPath());
        {
            DatasetGraph dg = TDB2Factory.connectDataset(location).asDatasetGraph();
            dg.close();
        }

        DependencyFilter classpathFlter = DependencyFilterUtils.classpathFilter(JavaScopes.COMPILE);
        CollectRequest collectRequest = new CollectRequest();
        for (org.apache.maven.model.Dependency dep : project.getDependencies()) {
            collectRequest.addDependency(new Dependency(new org.eclipse.aether.artifact.DefaultArtifact(
                    dep.getGroupId(), dep.getArtifactId(), dep.getClassifier(),
                    dep.getType(), dep.getVersion()), JavaScopes.COMPILE));
        }

        collectRequest.setRepositories(project.getRemoteProjectRepositories());
        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFlter);

        Set<String> includeTypeSet = new HashSet<>(Arrays.asList(includeTypes.split(",")));

        DependencyResult dependencyResult = repoSystem.resolveDependencies(repoSession, dependencyRequest);

        List<UpdateLoad> workloads = new ArrayList<>();
        for (ArtifactResult artifactResult : dependencyResult.getArtifactResults()) {
            Artifact artifact = artifactResult.getArtifact();
            String extension = artifact.getExtension();

            boolean accept = false;
            for (String suffix : includeTypeSet) {
                if (extension.endsWith(suffix)) {
                    int l = extension.length();
                    String tmp = extension.substring(0, l - suffix.length());
                    accept = tmp.isEmpty() || tmp.endsWith(".");
                }
            }

            if (!accept) {
                logger.debug("Ignoring " + artifact);
                continue;
            }

            File artifactFile = artifactResult.getArtifact().getFile();
            String artifactPath = artifactFile.getAbsolutePath();

            String graphName = "urn:mvn:" + toString(artifact);

            logger.info("Selecting TDB2 workload: " + artifactPath + " -> " + graphName);

            UpdateLoad update = new UpdateLoad(artifactPath, graphName);
            workloads.add(update);
        }

        // XXX This is a simple change detection procedure that needs to evolve in the future.
        // It does not handle the case where multiple files are mapped to the same graph.
        // In general, we need to create a metadata file, tdb instance or graph to do the book-keeping

        boolean[] change = {false};
        DatasetGraph dg = TDB2Factory.connectDataset(location).asDatasetGraph();
        try {
            for (UpdateLoad update : workloads) {
                Node destNode = update.getDest();
                logger.info("Preparing TDB2 workload: " + update.getSource() + " -> " + update.getDest());
                boolean isAlreadyLoaded = Txn.calculateRead(dg, () -> dg.containsGraph(destNode));
                if (isAlreadyLoaded) {
                    logger.info("Skipping TDB2 workload (already loaded): " + update.getSource() + " -> " + update.getDest());
                } else {
                    logger.info("Executing TDB2 workload: " + update.getSource() + " -> " + update.getDest());
                    Txn.executeWrite(dg, () -> {
                        UpdateExec.dataset(dg).update(update).execute();
                        change[0] = true;
                    });
                }
            }
        } finally {
            dg.close();
        }

        Path outputFolderPath = outputFolder.toPath().toAbsolutePath();

        Path tgtFile = outputFile.toPath().toAbsolutePath();
        Path tgtTmpFile = tgtFile.resolveSibling("." + tgtFile.getFileName().toString());

        if (Files.exists(tgtFile) && !change[0]) {
            logger.info("No changes detected. Archive already exists: " + tgtFile);
        } else {
            logger.info("Writing temp archive: " + tgtTmpFile);
            packageTdb2(logger::info, tgtTmpFile, outputFolderPath);
            atomicMoveOrCopy(logger::warn, tgtTmpFile, tgtFile);
            logger.info("Created archive: " + tgtFile);
        }

        mavenProjectHelper.attachArtifact(project, outputFile, "tdb2.tar.gz");
    }

    public static void atomicMoveOrCopy(Consumer<String> warnCallback, Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            warnCallback.accept(String.format("Atomic move from %s to %s failed, falling back to copy", source, target));
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static void packageTdb2(Consumer<String> fileCallback, Path fileToWrite, Path folderToPackage) throws IOException {
        try (OutputStream fOut = Files.newOutputStream(fileToWrite);
            BufferedOutputStream buffOut = new BufferedOutputStream(fOut);
            GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
            TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {

            Files.walkFileTree(folderToPackage, new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    // Skip lock files
                    if (!file.getFileName().toString().endsWith(".lock")) {
                        Path relPath =  folderToPackage.relativize(file);
                        TarArchiveEntry tarEntry = new TarArchiveEntry(file, relPath.toString());
                        tOut.putArchiveEntry(tarEntry);
                        fileCallback.accept("Adding: " + file);
                        Files.copy(file, tOut);
                        tOut.closeArchiveEntry();
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    throw new RuntimeException(exc);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });

            tOut.finish();
        }
    }

    protected String toString(Artifact coord) {
        String t = coord.getExtension();
        String c = coord.getClassifier();

        String suffix =
                (t == null || t.isEmpty() ? "" : ":" + t) +
                (c == null || c.isEmpty() ? "" : ":" + c);

        String result = coord.getGroupId() + ":" + coord.getArtifactId() + ":" + coord.getVersion() + suffix;
        return result;
    }
}
