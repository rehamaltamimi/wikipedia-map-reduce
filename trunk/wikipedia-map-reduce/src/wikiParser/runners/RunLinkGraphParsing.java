package wikiParser.runners;

import java.io.IOException;

public class RunLinkGraphParsing {

	public static void main(String[] args) throws IOException {
		System.out.println("RunInitialLinkMapReduce\n================================");
		RunInitialLinkMapReduce.run();
		System.out.println("RunArticleNameIdMapReduce\n================================");
		RunArticleNameIdMapReduce.run();
		System.out.println("RunUserNameIdMapReduce\n================================");
		RunUserNameIdMapReduce.run();
		System.out.println("RunNameIdSubstitution\n================================");
		RunNameIdSubstitution.run();
		System.out.println("RunCommutativeLinkMapReduce\n================================");
		RunCommutativeLinkMapReduce.run();
	}

}
