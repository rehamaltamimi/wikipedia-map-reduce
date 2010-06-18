package wikiParser;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class Graph {

	@Deprecated
	private List<Graph> subGraphs;
	private List<Edge> edges;
	
	public Graph() {
		this.edges = new ArrayList<Edge>();
	}
	
	public Graph(List<Edge> edges) {
		this.edges = new ArrayList<Edge>(edges);
	}
	
	public Graph(List<Page> articles, List<User> users) {
		TreeSet<Edge> uniqueEdges = new TreeSet<Edge>();
		for (Page each : articles) {
			uniqueEdges.addAll(each.getEdges());
		}
		for (User each : users) {
			uniqueEdges.addAll(each.getEdges());
		}
		this.edges = new ArrayList<Edge>(uniqueEdges);
	}

	public List<Edge> getEdges() {
		return edges;
	}
	
	public void addToEdges(Edge edge) {
		this.edges.add(edge);
	}

	public void setUsers(ArrayList<Edge> edges) {
		this.edges = edges;
	}
	
	public List<Vertex> getVertices() {
		TreeSet<Vertex> uniqueVertices = new TreeSet<Vertex>();
		for (Edge each : edges) {
			uniqueVertices.add(each.getOne());
			uniqueVertices.add(each.getTwo());
		}
		return new ArrayList<Vertex>(uniqueVertices);
	}
	
}
