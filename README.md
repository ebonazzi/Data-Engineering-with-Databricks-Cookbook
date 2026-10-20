# Data Engineering with Databricks Cookbook

<a href="https://www.packtpub.com/product/data-engineering-with-databricks-cookbook/9781837633357"><img src="https://content.packt.com/_/image/original/B19798/cover_image_large.jpg" alt="no-image" height="256px" align="right"></a>

This is the code repository for [Data Engineering with Databricks Cookbook](https://www.packtpub.com/product/data-engineering-with-databricks-cookbook/9781837633357), published by Packt.

**Build effective data and AI solutions using Apache Spark, Databricks, and Delta Lake**

## What is this book about?
This book shows you how to use Apache Spark, Delta Lake, and Databricks to build data pipelines, manage and transform data, optimize performance, and more. Additionally, you’ll implement DataOps and DevOps practices, and orchestrate data workflows.

This book covers the following exciting features:
* Perform data loading, ingestion, and processing with Apache Spark
* Discover data transformation techniques and custom user-defined functions (UDFs) in Apache Spark
* Manage and optimize Delta tables with Apache Spark and Delta Lake APIs
* Use Spark Structured Streaming for real-time data processing
* Optimize Apache Spark application and Delta table query performance
* Implement DataOps and DevOps practices on Databricks
* Orchestrate data pipelines with Delta Live Tables and Databricks Workflows
* Implement data governance policies with Unity Catalog

If you feel this book is for you, get your [copy](https://www.amazon.com/Engineering-Apache-Spark-Delta-Cookbook/dp/1837633355) today!

<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" 
alt="https://www.packtpub.com/" border="5" /></a>

## Instructions and Navigations
All of the code is organized into folders. For example, Chapter01.

The code will look like the following:
```
from pyspark.sql import SparkSession

spark = (SparkSession.builder
 .appName("read-csv-data")
 .master(«spark://spark-master:7077»)
 .config(«spark.executor.memory", "512m")
 .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
```

**Following is what you need for this book:**
This book is for data engineers, data scientists, and data practitioners who want to learn how to build efficient and scalable data pipelines using Apache Spark, Delta Lake, and Databricks. To get the most out of this book, you should have basic knowledge of data architecture, SQL, and Python programming.

With the following software and hardware list you can run all code files present in the book (Chapter 1-11).
### Software and Hardware List
| Chapter | Software required | OS required |
| -------- | ------------------------------------ | ----------------------------------- |
| 1-11 | Docker Engine version 18.02.0+ | Windows, Mac OS X, and Linux (any) |
| 1-11 | Docker Compose version 1.25.5+ | Windows, Mac OS X, and Linux (any) |
| 1-11 | Docker Desktop                 | Windows, Mac OS X, and Linux (any) |
| 1-11 | Git                            | Windows, Mac OS X, and Linux (any) |

## Running the local environment with rootless Podman

These notes cover running the lab environment on Linux with rootless Podman instead of Docker. The `docker` command is Podman's Docker shim, and `docker compose` runs the external `docker-compose` plugin against Podman's API socket.

### How the pieces fit together

- `build.sh` only **builds** the images: `base`, `spark-base`, `spark-master`, `spark-worker` and `jupyterlab`. It starts no containers.
- `docker-compose.yml` **starts** the environment: Zookeeper, Kafka, JupyterLab, one Spark master and two Spark workers. All containers share the repository folder, mounted at `/opt/workspace`.

### One-time setup

Enable the Podman API socket. `docker compose` cannot reach Podman without it, and fails with `failed to connect to the docker API at unix:///run/user/1000/podman/podman.sock`.

```bash
systemctl --user enable --now podman.socket
```

### Daily use

Run these commands from the repository root:

```bash
./build.sh                # only on first setup or after changing a Dockerfile
docker compose up -d      # start the environment
docker compose down       # stop the environment
```

- JupyterLab: http://localhost:8888. If it asks for a token, run `podman logs jupyterlab` to find it.
- Spark master UI: http://localhost:8080. It should list 2 alive workers.
- Spark application UI: http://localhost:4040, while a Spark session is running.
- In a notebook, connect to `spark://spark-master:7077`. See `docker/jupyterlab/00-first.py` for a Delta-enabled session.

### Harmless messages from build.sh

The cleanup functions at the start of `build.sh` try to remove old containers, images and the volume. On a clean system there is nothing to remove, so these messages appear and can be ignored:

- `Error: name or ID cannot be empty`
- `Error: parsing reference "": repository name must have at least one component`
- `Error: no volume with name "distributed-file-system" found`
- `[Warning] one or more build args were not consumed`

### Changes made to docker-compose.yml for Podman

| Change | Reason |
| ------ | ------ |
| Removed the top-level `version` key | Obsolete in the Compose specification and causes a warning. |
| Volume `device: ./` changed to `device: ${PWD}` | The relative path is passed to the engine unresolved and points to the wrong folder. Run `docker compose` from the repository root. |
| `docker.io/bitnami/*` changed to `docker.io/bitnamilegacy/*` | Bitnami removed the versioned Kafka and Zookeeper images from `docker.io/bitnami`. |
| Local images prefixed with `localhost/` | Podman tags locally built images as `localhost/<name>`. Without the prefix, compose tries to pull them from a registry. |
| `user: root` on `spark-master` and both workers | The images run as `NBuser`. Under rootless Podman, that user maps to an unprivileged host ID, so it cannot write to the shared folder. Container root maps to your own host user, so files written by Spark belong to you. |

`docker/base/Dockerfile` also pins the base image to `eclipse-temurin:17-jre-jammy` (Ubuntu 22.04) instead of `17-jre`, following the instructions on the book's GitHub repository.

### Troubleshooting

```bash
podman ps -a                                              # container status
podman logs <container>                                   # container output (e.g. kafka, jupyterlab)
podman exec spark-master cat logs/spark-master.out        # Spark master log
podman exec spark-worker-1 cat logs/spark-worker.out      # Spark worker log
systemctl --user status podman.socket                     # Podman API socket status
```

Quick check that the cluster works. Run this in a JupyterLab notebook:

```python
from delta import *
from pyspark.sql import SparkSession

builder = (SparkSession.builder
           .master("spark://spark-master:7077")
           .config("spark.executor.memory", "512m")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.range(100).write.format("delta").mode("overwrite").save("/opt/workspace/data/_smoketest")
print(spark.read.format("delta").load("/opt/workspace/data/_smoketest").count())   # expect 100
```

Delete `data/_smoketest` afterwards.

### Related products
* Business Intelligence with Databricks SQL [[Packt]](https://www.packtpub.com/product/business-intelligence-with-databricks-sql/9781803235332) [[Amazon]](https://www.amazon.com/Business-Intelligence-Databricks-SQL-intelligence/dp/1803235330/ref=sr_1_1?crid=1QYCAOZP9E3NH&dib=eyJ2IjoiMSJ9.nKZ7dRFPdDZyRvWwKM_NiTSZyweCLZ8g9JdktemcYzaWNiGWg9PuoxY2yb2jogGyK8hgRliKebDQfdHu2rRnTZTWZbsWOJAN33k65RFkAgdFX-csS8HgTFfjZj-SFKLpp4FC6LHwQvWr9Nq6f5x6eg.jh99qre-Hl4OHA9rypXLmSGsQp4exBvaZ2xUOPDQ0mM&dib_tag=se&keywords=Business+Intelligence+with+Databricks+SQL&qid=1718173191&s=books&sprefix=business+intelligence+with+databricks+sql%2Cstripbooks-intl-ship%2C553&sr=1-1)

* Optimizing Databricks Workloads [[Packt]](https://www.packtpub.com/product/optimizing-databricks-workloads/9781801819077) [[Amazon]](https://www.amazon.com/Optimizing-Databricks-Workloads-performance-workloads/dp/1801819076/ref=tmm_pap_swatch_0?_encoding=UTF8&dib_tag=se&dib=eyJ2IjoiMSJ9.cskfrEglx5gEbJF-FnhxlA.rCtKm1bO6Fi1mXUpq1Oai0kjAhGseGT2cCZ2Ccgxaak&qid=1718173341&sr=1-1)

## Get to Know the Author
**Pulkit Chadha**
 is a seasoned technologist with over 15 years of experience in data engineering. His proficiency in crafting and refining data pipelines has been instrumental in driving success across diverse sectors such as healthcare, media and entertainment, hi-tech, and manufacturing. Pulkit’s tailored data engineering solutions are designed to address the unique challenges and aspirations of each enterprise he collaborates with.

