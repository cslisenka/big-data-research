cd ../Tools/java-rest-binding
mvn clean install -Dmaven.test.skip=true

mvn install:install-file -Dfile=../ApprovalTests.jar -DgroupId=com.github.approvals -DartifactId=ApprovalTests -Dversion=0.13 -Dpackaging=jar
