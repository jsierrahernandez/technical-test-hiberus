# Technical-test-hiberus

## Overview

This project is designed to process large datasets using **Apache Spark** and **Scala**. It involves ETL (Extract, Transform, Load) operations to extract data, perform complex transformations, and load it into a target data store. The project uses the power of **distributed computing** to handle large-scale data processing.

## Features

- **Efficient Data Processing**: Uses **Apache Spark** to handle vast amounts of data.
- **Scalable Architecture**: Built to scale horizontally across many nodes.
- **Modular Design**: Organized into multiple modules for better maintainability and testing. Easy to extend and add new features like Docker (storage services, ...).

## Table of Contents

1. [Project Setup](#project-setup)
2. [Usage](#usage)


## Project Setup

To set up the project, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/username/project-name.git

2. Install dependencies:

   ```bash
   mvn install

3. Install dependencies:

   ```bash
   export SPARK_HOME=/path/to/spark
   export SCALA_HOME=/path/to/scala
   export PATH=$SPARK_HOME/bin:$SCALA_HOME/bin:$PATH

## How it works
Run application and get a propmt like this:

   ```bash
    2025-03-07 08:38:42,103 [main] INFO  App - Please choose an option (1, 2 or 3). Other values will be ignored.
    2025-03-07 08:38:42,103 [main] INFO  App - 1) Run your own query with Spark Sql
    2025-03-07 08:38:42,104 [main] INFO  App - 2) Run ETL
    2025-03-07 08:38:42,104 [main] INFO  App - 3) Exit
```

- Option 1 allows you to run your own query with Spark Sql.
- Option 2 runs the ETL process. 
- Option 3 exits the application.