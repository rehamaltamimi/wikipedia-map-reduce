All of these files are in the public macalesterpublic s3 bucket.

# Input Files #
Each wikipedia dump is split into 100 files named wikipedia.txt.xx with xx between 00 and 99.  The input has already been processed by split.py.  Each key is an article ID and each value is a compressed and escaped (to avoid line breaks and tabs from interfering with the formatting) full revision history of an individual article.
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/de-2010-04-05/** - German full revision history Wikipedia dump
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/en-2011-04-05/** - English full revision history Wikipedia dump
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/es-2010-04-04/** - Spanish full revision history Wikipedia dump
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/fr-2010-04-06/** - French full revision history Wikipedia dump
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/it-2010-04-11/** - Italian full revision history Wikipedia dump
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/ja-2010-03-31/** - Japanese full revision history Wikipedia dump

# Output #
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/citationStageOne/** - output from CitationCounter (English Wikipedia dump from 4/5/2011). Filenames are part-r-00000 to part-r-00039.
  * **https://s3.amazonaws.com/macalesterpublic/wikipedia/citeDomainOutput/output.txt** - output from CiteDomainCounter using the output from CitationCounter found above.

# Exploring S3 Files #
Since you don't own these files, you won't be able to view them through the AWS management console.  However, we have had good luck viewing them in [CyberDuck](http://cyberduck.ch/).  Create a new entry in cyberduck with anonymous login and specify macalesterpublic.s3.amazonaws.com as the server.