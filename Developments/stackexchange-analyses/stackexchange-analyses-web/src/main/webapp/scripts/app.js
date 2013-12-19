var app = angular.module('app', [
	'ngRoute', 'search-controller', 'circles-controller', 'main-controller'
]);

app.config(['$routeProvider', 
	function($routeProvider) {
    	$routeProvider
    		.when('/', {templateUrl: 'views/main-page.html', controller: 'MainPage'})
    		.when('/search/:experimentId', {templateUrl: 'views/clusters-search.html', controller: 'ClustersSearch'})
    		.when('/circles/:experimentId', {templateUrl: 'views/clusters-circles.html', controller: 'ClustersCircles'})
    		.otherwise({redirectTo: '/'});
	}
]);