var phonecatApp = angular.module('phonecatApp', ['ngRoute']);
 
phonecatApp.config(['$routeProvider', 
	function($routeProvider) {
    	$routeProvider
    		.when('/', {templateUrl: 'views/clusters-search.html', controller: 'ClustersCircles'})
    		.when('/circles', {templateUrl: 'views/clusters-circles.html', controller: 'ClustersSearch'})
    		.otherwise({redirectTo: '/'});
	}
]);