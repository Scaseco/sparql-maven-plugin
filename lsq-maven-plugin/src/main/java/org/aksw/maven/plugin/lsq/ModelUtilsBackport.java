package org.aksw.maven.plugin.lsq;

import java.util.Objects;
import java.util.stream.Stream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class ModelUtilsBackport {
    /** Create a union model of all unique non-null arguments */
    // XXX Order by size?
    public static Model union(Model ...models) {
        Model result = Stream.of(models)
                .filter(Objects::nonNull)
                .distinct()
                .reduce(ModelFactory::createUnion)
                .orElse(ModelFactory.createDefaultModel()); // Empty model?
        return result;
    }
}
