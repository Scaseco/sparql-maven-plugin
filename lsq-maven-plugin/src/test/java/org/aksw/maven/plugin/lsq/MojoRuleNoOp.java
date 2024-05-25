package org.aksw.maven.plugin.lsq;

import org.apache.maven.plugin.testing.MojoRule;

public class MojoRuleNoOp
    extends MojoRule
{
    @Override
    protected void before() throws Throwable {
    }

    @Override
    protected void after() {
    }
};
