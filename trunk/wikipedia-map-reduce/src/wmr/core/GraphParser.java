package wmr.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GraphParser {
	
	private ArrayList<Edge> edges;
	private BufferedReader reader;
	
	public GraphParser(String path) throws IOException {
		this.edges = new ArrayList<Edge>();
		reader = new BufferedReader(new FileReader(path));
	}
	
	public ArrayList<Edge> readGraph() throws IOException {
		String line = new String("");
		while (line != null) {
			line = reader.readLine();
			edges.addAll(processLine(line));
		}
		return edges;
	}

	protected static ArrayList<Edge> processLine(String line) {
		ArrayList<Edge> edges = null;
		Vertex e1 = null;
		e1 = extractEntity(line.split("\t")[0]);
		if (e1.equals(null)) {
			return null;
		}
		edges = extractEdges(line.split("\t")[1], e1);
		return edges;
	}

	protected static ArrayList<Edge> extractEdges(String valueString, Vertex e1) {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		String[] values = valueString.split(" ");
		Vertex e2 = null;
		for (String value : values) {
			e2 = extractEntity(value);
			if (e2.equals(null)) {
				return null;
			}
			int edgeType = Integer.parseInt(value.substring(1, 3));
			edges.add(new Edge(e1, e2, edgeType));
			/*
			 * 1. Determine entity type and create
			 * 2. Determine edge type
			 * 3. Create and add to list
			 */
		}
		return edges;
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
