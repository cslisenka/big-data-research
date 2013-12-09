package by.bsuir.kslisenko.action;

import java.util.ArrayList;
import java.util.List;

import by.bsuir.kslisenko.model.ClusteredDocument;
import by.bsuir.kslisenko.service.ClusteredDocumentService;
import by.bsuir.kslisenko.service.ServiceFactory;

import com.opensymphony.xwork2.ActionSupport;

public class GetDocumentsJsonAction extends ActionSupport {

	// Input
	private String clusterId;
	
	// Output
	private List<ClusteredDocument> documents = new ArrayList<ClusteredDocument>();
	
	public String index() {
		documents.clear();
		ClusteredDocumentService service = ServiceFactory.getDocumentService();
		documents.addAll(service.getClusteredDocuments(clusterId));
		return "success";
	}

	public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public List<ClusteredDocument> getDocuments() {
		return documents;
	}

	public void setDocuments(List<ClusteredDocument> documents) {
		this.documents = documents;
	}

	public String getJSON() {
		return index();
	}
}