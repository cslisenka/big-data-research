angular.module('topics', []).controller('ClustersListController', function ClustersListController($scope, $http) {
    $scope.getClusters = function() {
      $scope.loading = true;
      $http.get('getClustersJsonAction').success(function(data) {
          $scope.loading = false;
          $scope.clusters = data.clusters;
      }).error(function(data, status, headers, config) {
  		alert("error");
      });
    }
    
    $scope.getDocuments = function(clusterId, clusterName, index) {
      $scope.loading = true;
      $scope.selected = index;      
      $http.get('getDocumentsJsonAction?clusterId=' + clusterId).success(function(data) {
          $scope.loading = false;
          $scope.documents = data.documents;
          $scope.highlightWord(clusterName);
      }).error(function(data, status, headers, config) {
  		alert("error");
      });
    }
    
    $scope.highlightWord = function(clusterName) {
    	// TODO make this work
		$(".highlight-content").highlight(clusterName);
    }
    
    $scope.getClusters();
}).directive('highlight-word', function() {
// TODO make it work
});
