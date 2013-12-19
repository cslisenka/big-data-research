package by.bsuir.kslisenko.action;

import java.util.ArrayList;
import java.util.List;

import by.bsuir.kslisenko.model.Cluster;
import by.bsuir.kslisenko.service.ClusterService;
import by.bsuir.kslisenko.service.ServiceFactory;

import com.opensymphony.xwork2.ActionSupport;

public class GetClustersJsonAction extends ActionSupport {

	private List<Cluster> clusters = new ArrayList<Cluster>();
	private String experimentId;
	
	public String index() {
		clusters.clear();
		ClusterService service = ServiceFactory.getClusterService();
		clusters.addAll(service.getClusters(experimentId));
		return "success";
	}

	public List<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(List<Cluster> clusters) {
		this.clusters = clusters;
	}
	
	public String getExperimentId() {
		return experimentId;
	}

	public void setExperimentId(String experimentId) {
		this.experimentId = experimentId;
	}

	public String getJSON() {
		return index();
	}
}