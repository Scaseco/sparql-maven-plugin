package org.aksw.maven.plugin.jena;

import java.util.Map;

import org.aksw.jenax.annotation.reprogen.Iri;
import org.aksw.jenax.annotation.reprogen.KeyIri;
import org.aksw.jenax.annotation.reprogen.ResourceView;
import org.apache.jena.rdf.model.Resource;

@ResourceView
public interface LoadState
    extends Resource
{
    @KeyIri(LoadStateTerms.file)
    @Iri(LoadStateTerms.load)
    Map<String, FileState> getFileStates();
}
