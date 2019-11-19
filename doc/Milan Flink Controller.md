Milan provides a runtime infrastructure that executes applications on a Flink cluster.
This requires running a controller application on the cluster master node.

The controller is itself a Flink application, but it does not execute on the Flink cluster. Rather it executes as a normal JVM application using Flink's in-process mini cluster. This is because it relies on issuing "flink run" commands in order to execute Milan applications, and this can only be done on the master node.

The controller supports Flink running as a YARN application as well as non-YARN deployments.

### Requirements ###
The controller uses Kinesis streams to consume commands and report state and diganostics, so you must have already created these streams and they must be accessible from the master node of your Flink cluster.

The controller uses the local disk as a cache for application packages, which are usually about 100MB in size, so make sure you have plenty of space available.

### Building the Flink Controller ###
From the root folder, run "mvn clean package -Dpackageflink=true -DskipTests".
It's necessary to package the Flink libraries into the controller jar because the version of Flink on the cluster can be different than the one required by the controller app.
This will produce a jar at `milan/milan-lang/target/milan-lang-1.0-SNAPSHOT.jar` that contains the controller application and all dependencies.

### Executing the Controller ###
Copy the jar above onto the master node of your flink cluster.

Execute the controller, filling in the names of the Kinesis streams:
```
java -classpath milan-lang-1.0-SNAPSHOT.jar com.amazon.milan.flink.control.ControllerFlinkApp --message-input-stream [controller messages stream name] --state-output-stream [controller state stream name] --diagnostic-output-stream [diagnostics stream name] --package-cache-folder /tmp/milanpackages --yarn --s3-package-repo s3://ml-cam-storage/doa/demo/packagerepo
```
If Flink is not running as a YARN app then remove the `--yarn` flag.

### Maintenance ###
At the moment the package cache (`/tmp/milanpakages` in the example above) is never cleaned, therefore it is necessary to manually remove unused application packages occasionally otherwise the disk on your master node will fill up.

To stop the controller:
1. Run `ps -C java -f | grep "milan-lang-1.0-SNAPSHOT.jar"` to list the java process for the controller jar.
1. `sudo kill -9` this process.
