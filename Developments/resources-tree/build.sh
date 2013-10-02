mvn clean install -Dmaven.test.skip=true
cd resources-tree-hadoop-nodes
mvn assembly:assembly -Dmaven.test.skip=true
cd ../resources-tree-hadoop-relationships
mvn assembly:assembly -Dmaven.test.skip=true
echo "created resources-tree-hadoop-relationships/target/resource-tree-nodes-jar-with-dependencies.jar"
echo "created resources-tree-hadoop-nodes/target/resource-tree-nodes-jar-with-dependencies.jar"
