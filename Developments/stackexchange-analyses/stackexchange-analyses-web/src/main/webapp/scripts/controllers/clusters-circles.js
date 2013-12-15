phonecatApp.controller('ClustersCircles', ['$scope', '$routeParams',
	function($scope, $routeParams) {
		$("#pleaseWait").modal('show');
		fillCircles();
	}
]);

function fillCircles() {
	var diameter = 1600, format = d3.format(",d"), color = d3.scale.category20c();
	var bubble = d3.layout.pack()
		.sort(null)
	    .size([diameter, diameter])
	    .padding(1.5);
	
	var svg = d3.select("#clusters").append("svg")
		.attr("width", diameter)
		.attr("height", diameter)
	    .attr("class", "bubble");
	
	d3.json("json/getClustersJsonAction", function(error, root) {
	  var node = svg.selectAll(".node")
	      .data(bubble.nodes(myClasses(root))
	      .filter(function(d) { return !d.children; }))
	    .enter().append("g")
	      .attr("class", "node")
	      .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
	
	  node.append("title")
	      .text(function(d) { return d.className + ": " + format(d.value); });
	
	  node.append("circle")
	      .attr("r", function(d) { return d.r; })
	      .style("fill", function(d) { return color(d.packageName); });
	
	  node.append("text")
	      .attr("dy", ".3em")
	      .style("text-anchor", "middle")
	      .text(function(d) { return d.className.substring(0, d.r / 3); });
	      
	 $("#pleaseWait").modal('hide');
	});
	
	function myClasses(root) {
		var classes = [];
	
		root.clusters.forEach(function(entry) {
			classes.push({packageName: entry.name, className: entry.name + '(' + entry.numPoints + ')', value: entry.numPoints});
		});
	  
		return {children: classes};
	}
	
	d3.select(self.frameElement).style("height", diameter + "px");
}