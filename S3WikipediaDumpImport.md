# Useful Unix Commands #
  * _trap "" HUP_: Run even if the terminal closes
  * _>& file_: puts output and error messages in file
  * _command &_: runs command in the background

# Getting GNU parallel #

The latest scripts require GNU's parallel command. Get it from the "Download package" link on the project's [Homepage](https://build.opensuse.org/package/show?package=parallel&project=home%3Atange).

# Importing Wikipedia Dumps to S3 #
  * Use encode.sh to create a new version that is encoded so it is compatible with Hadoop. This can take several days to run, so it is important to take precautions in case the terminal window closes or the connection is broken for some reason. Using >& to put error and log messages into a file is also recommended.  Using a trailing & allows the task to run in the background. Example command:
> > `trap "" HUP && ./encode.sh en-20110405/ en-20110405/enwiki-20110405-pages-meta-history1.xml.7z en-20110405/enwiki-20110405-pages-meta-history2.xml.7z en-20110405/enwiki-20110405-pages-meta-history3.xml.7z en-20110405/enwiki-20110405-pages-meta-history4.xml.7z en-20110405/enwiki-20110405-pages-meta-history5.xml.7z en-20110405/enwiki-20110405-pages-meta-history6.xml.7z en-20110405/enwiki-20110405-pages-meta-history7.xml.7z en-20110405/enwiki-20110405-pages-meta-history8.xml.7z en-20110405/enwiki-20110405-pages-meta-history9.xml.7z en-20110405/enwiki-20110405-pages-meta-history10.xml.7z en-20110405/enwiki-20110405-pages-meta-history11.xml.7z en-20110405/enwiki-20110405-pages-meta-history12.xml.7z en-20110405/enwiki-20110405-pages-meta-history13.xml.7z en-20110405/enwiki-20110405-pages-meta-history14.xml.7z en-20110405/enwiki-20110405-pages-meta-history15.xml.7z >&log &  `

> You can also add the keyword "current" to only encode the current revision:
> > `trap "" HUP && ./encode.sh *current* en-20110405/ en-20110405/enwiki-20110405-pages-meta-history1.xml.7z ... `
  * Upload files to s3:
> > `./upload.sh s3://macalester/wikipedia/en-2011-04-05/ ./en-20110405/wikipedia.txt.[0-9][0-9]`