var phonecatApp = angular.module('phonecatApp', ['ngRoute']);
 
phonecatApp.config(['$routeProvider', 
	function($routeProvider) {
    	$routeProvider
    		.when('/', {templateUrl: 'views/main-page.html', controller: 'MainPage'})
    		.when('/photo/search', {templateUrl: 'views/clusters-search.html'})
    		.when('/photo/circles', {templateUrl: 'views/clusters-circles.html'})
    		.otherwise({redirectTo: '/'});
	}
]);