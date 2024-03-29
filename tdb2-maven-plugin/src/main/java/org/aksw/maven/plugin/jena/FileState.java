package org.aksw.maven.plugin.jena;

import java.util.Set;

import org.aksw.jenax.annotation.reprogen.Iri;
import org.aksw.jenax.annotation.reprogen.ResourceView;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;

@ResourceView
public interface FileState
    extends Resource
{
    @Iri(LoadStateTerms.file)
    String getFileName();
    FileState setFileName();

    // The graph names into which the file was loaded. DEFAULT for default graph.
    @Iri(LoadStateTerms.graph)
    Set<Node> getGraphs();
}
