package org.aksw.maven.plugin.lsq;

import java.io.File;

import org.aksw.commons.util.derby.DerbyUtils;
import org.apache.jena.sys.JenaSystem;
import org.apache.maven.plugin.Mojo;
import org.apache.maven.plugin.testing.MojoRule;
import org.apache.maven.project.MavenProject;
import org.junit.Rule;
import org.junit.Test;

public class TestLsqRdfizeMojo {

    static {
        DerbyUtils.disableDerbyLog();
        JenaSystem.init();
    }

    @Rule
    public MojoRule rule = new MojoRuleNoOp();

    @Test
    public void testLsqRdfize01() throws Exception {
        File file = new File("src/test/resources/lsq/test-rdfize-01");
        MavenProject project = rule.readMavenProject(file);
        Mojo mojo = rule.lookupConfiguredMojo(project, "rdfize");
        mojo.execute();
    }
}
