package org.aksw.maven.plugins.rpt;

import org.aksw.maven.plugin.rpt.RmlMojo;
import org.apache.jena.sys.JenaSystem;
import org.apache.maven.plugin.testing.MojoRule;
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
        RmlMojo mojo = (RmlMojo) rule.lookupMojo("rml", "src/test/resources/rml/test-gtfs-01/pom.xml");
        mojo.execute();
    }
}
