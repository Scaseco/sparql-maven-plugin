package org.aksw.maven.plugin.rpt;

import java.io.File;

import org.apache.jena.sys.JenaSystem;
import org.apache.maven.plugin.Mojo;
import org.apache.maven.plugin.testing.MojoRule;
import org.apache.maven.project.MavenProject;
import org.junit.Rule;
import org.junit.Test;

public class TestRptMojo {

    static { JenaSystem.init(); }

    @Rule
    public MojoRule rule = new MojoRule() {
        @Override
        protected void before() throws Throwable {
        }

        @Override
        protected void after() {
        }
    };

    @Test
    public void testMojoGoal() throws Exception {
        File file = new File("src/test/resources/rml/test-gtfs-01");
        MavenProject project = rule.readMavenProject(file);
        Mojo mojo = rule.lookupConfiguredMojo(project, "rml");
        mojo.execute();
    }
}
