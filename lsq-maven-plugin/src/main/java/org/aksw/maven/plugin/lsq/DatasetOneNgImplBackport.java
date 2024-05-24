package org.aksw.maven.plugin.lsq;

import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.dataset.impl.DatasetGraphOneNgImpl;
import org.aksw.jenax.arq.dataset.impl.DatasetOneNgImpl;
import org.apache.jena.rdf.model.RDFNode;

public class DatasetOneNgImplBackport {
    public static DatasetOneNg naturalDataset(RDFNode resource) {
        return new DatasetOneNgImpl(DatasetGraphOneNgImpl.create(resource.asNode(), resource.getModel().getGraph()));
    }
}
