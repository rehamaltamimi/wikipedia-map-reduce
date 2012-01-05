package wmr.core;

public class AnonymousUser extends User {
	
	private String ip;
	
	public AnonymousUser(String ip) {
		super();
		this.ip = ip;
	}
	
	public String getIp() {
		return this.ip;
	}
	
	public void setIp(String ip) {
		this.ip = ip;
	}

}
