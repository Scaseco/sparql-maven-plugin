package org.aksw.maven.plugin.rpt;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aksw.commons.model.maven.domain.api.MavenEntityCore;
import org.aksw.commons.model.maven.domain.impl.MavenEntityCoreImpl;
import org.aksw.commons.util.derby.DerbyUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.UpdateExec;
import org.apache.jena.sparql.modify.request.UpdateLoad;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Component;
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

public class Tdb2MojoShared extends AbstractMojo {

    static { DerbyUtils.disableDerbyLog(); }

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

    /** Comma separated list of dependency types which to include */
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

        logger.info("TDB2 loader started");

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

            String graphName = "urn:mvn:" + MavenEntityCore.toString(new MavenEntityCoreImpl(artifact.getGroupId(), artifact.getArtifactId(), artifact.getVersion(), artifact.getExtension(), artifact.getClassifier()));

            logger.info("Selecting TDB2 workload: " + artifactPath + " -> " + graphName);

            UpdateLoad update = new UpdateLoad(artifactPath, graphName);
            workloads.add(update);
            // MavenEntity artifact = new MavenEntityCoreImpl(filename, filename, filename, filename, filename);
            // out artifactResult..toString();

            // Files.copy(artifactFile.toPath(), outputDirectory.toPath().resolve(filename));
        }

        DatasetGraph dg = TDB2Factory.connectDataset(location).asDatasetGraph();
        try {
            Txn.executeWrite(dg, () -> {
                for (UpdateLoad update : workloads) {
                    logger.info("Executing TDB2 workload: " + update.getSource() + " -> " + update.getDest());
                    UpdateExec.dataset(dg).update(update).execute();
                }
            });
        } finally {
            dg.close();
        }

        try (OutputStream fOut = Files.newOutputStream(outputFile.toPath());
            BufferedOutputStream buffOut = new BufferedOutputStream(fOut);
            GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(buffOut);
            TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {

            Files.walkFileTree(outputFolder.toPath(), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    // Skip lock files
                    if (!file.getFileName().endsWith(".lock")) {
                        TarArchiveEntry tarEntry = new TarArchiveEntry(file, file.getFileName().toString());
                        tOut.putArchiveEntry(tarEntry);
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

        mavenProjectHelper.attachArtifact(project, outputFile, "tdb2.gz");
    }
}
