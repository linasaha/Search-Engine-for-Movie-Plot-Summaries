# Search-Engine-for-Movie-Plot-Summaries

In this part, we will work with a dataset of movie plot summaries that is available from the 'Carnegie Movie Summary Corpus' site. We are interested in building a search engine for the plot summaries that are available in the file "plot summaries.txt" (can be downloaded from 'Carnegie Movie Summary Corpus' site).

URL for data: http://www.cs.cmu.edu/~ark/personas/data/MovieSummaries.tar.gz

Built a search engine for the plot summaries.

Technique used: TF-IDF. Eliminated stop words and created a tf-idf for every term and every document using the MapReduce method.

For the search terms entered by the user, a list of movie names sorted by their tf-idf values in descending order is returned.

This assignment is done using Scala code that can run on a Spark cluster. A project was created using IntelliJ Scala and the executable jar file can be run on a AWS EMR.
