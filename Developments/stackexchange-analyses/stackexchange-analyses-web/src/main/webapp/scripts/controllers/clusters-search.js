phonecatApp.controller('ClustersSearch', ['$scope', '$http',
	function ($scope, $http) {
		$scope.getClusters = function() {
			$scope.loading = true;
			$http.get('json/getClustersJsonAction').success(function(data) {
				$scope.loading = false;
				$scope.clusters = data.clusters;
			}).error(function(data, status, headers, config) {
				alert("error");
			});
    	}
    
	    $scope.getDocuments = function(clusterId, clusterName, index) {
			$scope.loading = true;
			$scope.selected = index;      
			$http.get('json/getDocumentsJsonAction?clusterId=' + clusterId).success(function(data) {
				$scope.loading = false;
				$scope.documents = data.documents;
			}).error(function(data, status, headers, config) {
				alert("error");
			});
	    }
    
    	$scope.getClusters();
	}
]);

