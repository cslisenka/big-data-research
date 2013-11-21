package by.bsuir.kslisenko.action;

import java.util.ArrayList;
import java.util.List;

import by.bsuir.kslisenko.model.Cluster;

import com.opensymphony.xwork2.ActionSupport;

public class GetClustersJsonAction extends ActionSupport {

	private List<Cluster> clusters = new ArrayList<Cluster>();
	
	public String index() {
		Cluster cl1 = new Cluster();
		cl1.setName("Cluster1");
		cl1.setNumPoints(50);
		
		Cluster cl2 = new Cluster();
		cl2.setName("Cluster 2");
		cl2.setNumPoints(40);		

		Cluster cl3 = new Cluster();
		cl3.setName("Cluster 3");
		cl3.setNumPoints(60);		
		
		clusters.add(cl1);
		clusters.add(cl2);
		clusters.add(cl3);
		
		return "success";
	}

	public List<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(List<Cluster> clusters) {
		this.clusters = clusters;
	}
	
	public String getJSON() {
		return index();
	}
}