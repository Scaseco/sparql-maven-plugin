package org.aksw.maven.plugin.lsq;

import java.io.File;

import org.aksw.jenax.dataaccess.sparql.factory.dataengine.RdfDataEngines;
import org.aksw.jenax.stmt.core.SparqlStmtParserImpl;
import org.aksw.jenax.web.server.boot.ServerBuilder;
import org.aksw.jenax.web.server.boot.ServletBuilderSparql;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.maven.plugin.Mojo;
import org.apache.maven.plugin.testing.MojoRule;
import org.apache.maven.project.MavenProject;
import org.eclipse.jetty.server.Server;
import org.junit.Rule;
import org.junit.Test;

public class TestLsqBenchmarkMojo {

    static { JenaSystem.init(); }

    @Rule
    public MojoRule rule = new MojoRuleNoOp();

    @Test
    public void testLsqBenchmark01() throws Exception {
        int port = 8154;

        // If there is a '"dcs" is null' exception in glassfish then there is a
        // dependency and/or service transformer conflict.
        // Probably because lsq includes a shaded sansa with jetty and glassfish.
        // The following snippet should provoke the issue without the need to start jetty.
        //
        // ServiceLocator serviceLocator = ServiceLocatorFactory.getInstance().create("test");
        // DynamicConfigurationService dcs = serviceLocator.getService(DynamicConfigurationService.class);
        // DynamicConfiguration config = dcs.createDynamicConfiguration();
        // serviceLocator.shutdown();

        Server server = ServerBuilder.newBuilder().addServletBuilder(
                ServletBuilderSparql.newBuilder()
                    .setSparqlStmtParser(SparqlStmtParserImpl.create())
                    .setSparqlServiceFactory(RdfDataEngines.of(DatasetFactory.empty())))
            .setPort(port).create();

        try {
            server.start();

            File file = new File("src/test/resources/lsq/test-benchmark-01");
            MavenProject project = rule.readMavenProject(file);
            Mojo mojo = rule.lookupConfiguredMojo(project, "benchmark");
            mojo.execute();
        } finally {
            server.stop();
        }
    }
}
