Wikipedia-map-reduce is a java software library that allows analysis of Wikipedia at the revision-text level.  The library uses the [Hadoop](http://hadoop.apache.org/) map-reduce framework for parallel processing.  It supports both standard Hadoop clusters and [Amazon's Elastic Map-Reduce](http://aws.amazon.com/elasticmapreduce/).

This library grew out of Colin Welch's honors project and is in development.  It's rough, but mostly functional.

If you're looking for a good place to start, check out these wiki pages:
  * PubliclyAvailableFiles: a list of files we've made public, including full revision history Wikipedia dumps for six languages
  * KarmasphereSetUp: for information on how to set up Karmasphere and get it to work with s3.
  * NotesOnHadoopJobs: a few notes about our experiences running Hadoop jobs, especially those using the full revision history Wikipedia dumps as input
  * S3WikipediaDumpImport: for information about how to import a Wikipedia dump to s3.  We made our own processed dumps available at PubliclyAvailableFiles.