Generation of Data Sets
=======================

# References
* [This GitHub repository](https://github.com/bom4v/ti-spark-data-generation)
  is a component of the [BOM4V project](https://github.com/bom4v/metamodels),
  aiming at demonstrating end-to-end Spark-based examples
  of Machine Learning (ML) pipelines, for instance
  churn detection in telecoms and transport industries.
* [Central Maven repository with BOM4V Jar artefacts](https://repo1.maven.org/maven2/org/bom4v/ti/)
* [Docker cloud with ready-to-use images](https://cloud.docker.com/u/bigdatadevelopment/repository/docker/bigdatadevelopment/base)

## Telecoms
* Generation of CDR: https://github.com/RealImpactAnalytics/cdr-generator/tree/master/src/main/scala/Model

# Installation

## Short version
Just add the dependency on `ti-spark-data-generation` in the SBT project
configuration (typically, `build.sbt` in the project root directory):
```scala
libraryDependencies += "org.bom4v.ti" %% "ti-spark-data-generation" % "0.0.1-spark2.3"
```

