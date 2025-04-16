# 📊 MapReduce Project: Author Group Size Analysis

This Java-based project implements a two-phase **MapReduce** algorithm to analyze author data. The goal is to determine:
1. The **average size** of author groups.
2. The **number of groups** smaller than the average size.

> 💡 Developed as part of a Big Data coursework at the University of Macedonia, Thessaloniki, November 2024.

---

## 👥 Authors

- **Paraskevi Tsormpari**
- **Foulidis Dimitrios** 

---

## 🧰 Tech Stack
- **Java**
- **Apache Hadoop framework**
- **HDFS** (Hadoop Distributed File System) and **YARN** (Yet Another Resource Negotiator)

---

## 🧠 Problem Description

We analyze co-author records given in a dataset in CSV format. 
Each `authors` field includes names separated by a pipe `|`. The analysis is performed in **two MapReduce cycles**.

## ✅ Example Input

```txt
1442405;Anna Bernasconi|Carsten Damm|Igor Shparlinski;;;;;
2456312;John Smith|Maria Gonzalez|Ahmed Khan|Oliver Brown|Su Wei;;;;;
3478924;Emily Davis|Hiroshi Tanaka;;;;;
```

## ✅ Sample Output (Final)

```txt
Number of teams below average size, 1133676
```

---

### 🔁 First Cycle: Unique Team Collection (deduplication) & Average Group Size Data

- **Mapper 1**:
  - Extracts the authors and the size of each group.
  - Emits key-value pairs: `team_string -> group_size. Example: “Anna Bernasconi|Carsten Damm|Igor Shparlinski”, 3`.

- **Reducer 1**:
  - Deduplicates identical author groups.
  - Calculates:
    - Total number of unique groups.
    - Total number of every group's authors.
    - Outputs each unique group and its size. Also retains the total group and author count for the Average Group Size Calculation in the run configuration

### 🧮 Second Cycle: Groups Below Average

- **Mapper 2**:
  - Calculates average group size and filters groups with size **less than the average**.
  - Emits: `BelowAverage -> 1`.

- **Reducer 2**:
  - Sums all `1`s to produce:
    - `Number of teams below average size -> total_count`.

---

## 🛠️ Technologies Used

- **Java**
- **Apache Hadoop**

No external libraries were used beyond standard Hadoop dependencies.

---

## ⚙️ Setup & Execution

### 🔧 Pre-requisites

- Java 8+
- Hadoop installed and configured

### 🧪 Local Build

1. Clone/Download the repository.
3. Open the project and Export the JAR via Eclipse.

### 🧰 HDFS Preparation

1. Start Hadoop services:

```bash
start-dfs.sh
start-yarn.sh
```

2. Enable passwordless SSH (if not set up):


3. Upload input files to HDFS:

```bash
hdfs dfs -put /localdir /hdfsdir
```

### 🚀 Run the Job

```bash
hadoop jar BigDataMapReduce.jar App /hdfsInputfileDir /hdfsIntermediateOutputDir /hdfsFinalOutputDir
```

> Replace paths with actual HDFS paths on your setup.

---

## 📈 Performance Results

| Run | 1 Reduce Task (ms) | 2 Reduce Tasks (ms) |
|-----|---------------------|---------------------|
| 1   | 33,754              | 26,309              |
| 2   | 30,019              | 24,751              |
| 3   | 28,793              | 30,377              |
| 4   | 42,759              | 25,253              |
| 5   | 24,825              | 26,537              |
| **Average** | **30,855.33** | **26,033.00** |

### 📊 Observations

- **15.63% speed improvement** when using 2 Reducers.
- **Reduced execution variance** (17934ms vs 5626ms), implying better load balancing.

---

## 📌 Notes

- Increase VM disk space to **30GB** to avoid storage issues.
- Ensure input files are UTF-8 encoded and follow the expected format.

---

## 📚 License

This project is developed for educational purposes and is not intended for commercial use.
