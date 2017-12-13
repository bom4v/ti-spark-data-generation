Generation of Data Sets
=======================

# Build and deployment
See http://github.com/bom4v/metamodels for the way to build, package and deploy
all the Telecoms Intelligence (TI) Models components, including that project.

# Run the demonstrator
```bash
$ mkdir -p ~/dev/ti
$ cd ~/dev/ti
$ git clone https://github.com/bom4v/metamodels.git
$ cd metamodels
$ rake clone && rake checkout
$ rake offline=true deliver
$ cd workspace/src/ti-spark-data-generation
$ ./fillLocalDataDir.sh
$ sbt run
[info] Loading global plugins from ~/.sbt/0.13/plugins
[info] Loading project definition from ~/dev/ti/metamodels/workspace/src/ti-spark-data-generation/project
[info] Set current project to ti-spark-data-generation (in build file:~/dev/ti/metamodels/workspace/src/ti-spark-generation/)
[info] Compiling 1 Scala source to ~/dev/ti/metamodels/workspace/src/ti-spark-data-generation/target/scala-2.10/classes...
[info] Running org.bom4v.ti.Demonstrator

[success] Total time: 2 s, completed Dec 14, 2017 6:04:35 PM
```

