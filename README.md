# rpt-maven-plugin

This is a maven plugin wrapper based on the [RDF Processing Toolkit](https://github.com/SmartDataAnalytics/RdfProcessingToolkit).

It features goals for SPARQL and RML execution.

This plugin comes in two flavors:

* `rpt-full-maven-plugin`: This variant supports many special features at the cost of a large download (~500MB).
* `rpt-maven-plugin`: This is a version which has some niche features stripped in order to significantly reduce the plugin size. It is a strict subset of `rpt-full-maven-plugin`.

*Currently only `rpt-full-maven-plugin` is functional*

Typically, `rpt-maven-plugin` should be sufficient for most tasks.

`rpt-full-maven-plugin` includes a shaded version of Apache Spark which can be used to ingest certain input formats (multiline CSV, trig, JSON arrays, JSON sequences) in parallel.




