#!/bin/bash


src_files="
src/wikiParser/categories/CategoryComparer.java
src/wikiParser/categories/CategoryRecord.java
src/wikiParser/categories/LocalCategoryComparer.java
"
javac -cp lib/trove-3.0.2.jar $src_files &&
java -server -Xmx3000M -cp lib/trove-3.0.2.jar:src wikiParser.categories.LocalCategoryComparer dat/all_cats.txt stdout 10 |
pbzip2 -m5000 -p20 >dat/cat_page_sims.txt.bz2
