var searchController = angular.module('search-controller', []);
searchController.controller('ClustersSearch', ['$scope', '$routeParams', '$http',
	function($scope, $routeParams, $http) {
		$scope.getClusters = function(experimentId) {
			$("#pleaseWait").modal('show');
			$http.get('json/getClustersJsonAction?experimentId=' + experimentId).success(function(data) {
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
    
    	$scope.getClusters($routeParams.experimentId);
	}
]);