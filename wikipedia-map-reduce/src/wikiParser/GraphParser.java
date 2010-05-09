package wikiParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GraphParser {
	
	/**
	 * @uml.property  name="links"
	 */
	private ArrayList<Edge> links;
	/**
	 * @uml.property  name="reader"
	 */
	private BufferedReader reader;
	
	public GraphParser(String path) throws IOException {
		this.links = new ArrayList<Edge>();
		reader = new BufferedReader(new FileReader(path));
	}
	
	public ArrayList<Edge> readGraph() throws IOException {
		String line = new String("");
		while (line != null) {
			line = reader.readLine();
			links.addAll(processLine(line));
		}
		return links;
	}

	protected static ArrayList<Edge> processLine(String line) {
		ArrayList<Edge> links = null;
		Vertex e1 = null;
		e1 = extractEntity(line.split("\t")[0]);
		if (e1.equals(null)) {
			return null;
		}
		links = extractLinks(line.split("\t")[1], e1);
		return links;
	}

	protected static ArrayList<Edge> extractLinks(String valueString, Vertex e1) {
		ArrayList<Edge> links = new ArrayList<Edge>();
		String[] values = valueString.split(" ");
		Vertex e2 = null;
		for (String value : values) {
			e2 = extractEntity(value);
			if (e2.equals(null)) {
				return null;
			}
			int linkType = Integer.parseInt(value.substring(1, 3));
			links.add(new Edge(e1, e2, linkType));
			/*
			 * 1. Determine entity type and create
			 * 2. Determine link type
			 * 3. Create and add to list
			 */
		}
		return links;
	}

	protected static Vertex extractEntity(String key) {
		if (key.startsWith("a")) {
			Page article = new Page(key.substring(1));
			return article;
		}
		else if (key.startsWith("u")) {
			User user = new User(key.substring(1));
			return user;
		}
		else {
			return null;
		}
	}
}
