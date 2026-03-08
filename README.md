# 🚀 Airflow 3.0 Data Engineering Sandbox

Welcome to my modernized Apache Airflow 3.0 environment. This repository serves as a robust, scalable foundation for building, testing, and deploying data pipelines within a **WSL2** and **Docker** ecosystem.



## 🛠 Tech Stack
* **Orchestrator:** Apache Airflow 3.0 (The next generation of orchestration)
* **Package Manager:** [uv](https://astral.sh/uv) (Extremely fast Python dependency management)
* **Infrastructure:** Docker & Docker Compose
* **Environment:** WSL2 (Ubuntu)

## 🏗 Project Structure
```text
.
├── dags/               # My Python DAG definitions
├── config/             # Airflow configuration files
├── plugins/            # Custom operators and hooks
├── docker-compose.yaml # Orchestration of the Airflow stack
└── pyproject.toml      # Project dependencies managed by uv
