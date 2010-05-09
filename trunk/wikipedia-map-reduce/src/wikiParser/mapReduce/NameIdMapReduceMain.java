package wikiParser.mapReduce;

import java.io.IOException;

import wikiParser.mapReduce.graphs.ArticleNameIdMapReduce;
import wikiParser.mapReduce.graphs.UserNameIdMapReduce;

public class NameIdMapReduceMain {

	public static void main (String [] args) throws IOException {
		ArticleNameIdMapReduce.runMe(
                        new String[] {"/user/shilad/wikipedia.txt"},
                        "articleNameIdOutput", "nameIdMapReduce"
                    );
		UserNameIdMapReduce.runMe(
                        new String[] {"/user/shilad/wikipedia.txt"},
                        "userNameIdOutput", "nameIdMapReduce"
                    );
	}
	
}
