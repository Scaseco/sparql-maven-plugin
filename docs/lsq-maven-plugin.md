---
layout: default
title: Overview
nav_order: 10
---

# LSQ Maven Plugin

The [Linked SPARQL Queries](https://lsq.aksw.org/) (LSQ) Maven Plugin features the goals:

* `rdfize`: Extracts SPARQL queries from Web server logs (e.g. Apache, Virtuoso) and emits quad-based RDF data. Each graph corresponds to one query.
* `benchmark`: Takes a SPARQL query log as input and runs the queries against a configured endpoint.
   The query log may be supplied in various formats, including that of the output of `rdfize`.


