package wikiParser;

import java.util.ArrayList;

public class Graph {

	/**
	 * @uml.property  name="subGraphs"
	 */
	private ArrayList<Graph> subGraphs;
	/**
	 * @uml.property  name="articles"
	 */
	private ArrayList<Page> articles;
	/**
	 * @uml.property  name="users"
	 */
	private ArrayList<User> users;
	
	public Graph() {
		this.subGraphs = new ArrayList<Graph>();
		this.articles = new ArrayList<Page>();
		this.users = new ArrayList<User>();
	}
	
	public Graph(ArrayList<Graph> subGraphs, ArrayList<Page> articles, ArrayList<User> users) {
		this.subGraphs = subGraphs;
		this.articles = articles;
		this.users = users;
	}

	public ArrayList<Page> getArticles() {
		return articles;
	}

	public void setArticles(ArrayList<Page> articles) {
		this.articles = articles;
	}

	public ArrayList<Graph> getSubGraphs() {
		return subGraphs;
	}

	public void setSubGraphs(ArrayList<Graph> subGraphs) {
		this.subGraphs = subGraphs;
	}

	public ArrayList<User> getUsers() {
		return users;
	}

	public void setUsers(ArrayList<User> users) {
		this.users = users;
	}
	
}
