phonecatApp.controller('ClustersSearch', ['$scope', '$http',
	function ($scope, $http) {
		$scope.getClusters = function() {
			$("#pleaseWait").modal('show');
			$http.get('json/getClustersJsonAction').success(function(data) {
				$("#pleaseWait").modal('hide');
				$scope.clusters = data.clusters;
			}).error(function(data, status, headers, config) {
				alert("error");
			});
    	}
    
	    $scope.getDocuments = function(clusterId, clusterName, index) {
			$("#pleaseWait").modal('show');
			$scope.selected = index;      
			$http.get('json/getDocumentsJsonAction?clusterId=' + clusterId).success(function(data) {
				$("#pleaseWait").modal('hide');
				$scope.documents = data.documents;
			}).error(function(data, status, headers, config) {
				alert("error");
			});
	    }
    
    	$scope.getClusters();
	}
]);