package by.bsuir.kslisenko.action;

import java.util.ArrayList;
import java.util.List;

import by.bsuir.kslisenko.model.Cluster;
import by.bsuir.kslisenko.service.ClusterService;
import by.bsuir.kslisenko.service.ServiceFactory;

import com.opensymphony.xwork2.ActionSupport;

public class GetClustersJsonAction extends ActionSupport {

	private List<Cluster> clusters = new ArrayList<Cluster>();
	
	public String index() {
		clusters.clear();
		ClusterService service = ServiceFactory.getClusterService();
		clusters.addAll(service.getClusters());
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