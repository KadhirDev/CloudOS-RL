# ☁️ CloudOS-RL — AI Multi-Cloud Scheduler

> **PPO Reinforcement Learning** scheduler that optimizes cloud workload placement across cost, latency, carbon footprint, and SLA — with real-time explainability.

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Native-326CE5?style=flat&logo=kubernetes&logoColor=white)](https://kubernetes.io)
[![React](https://img.shields.io/badge/React-Vite-61DAFB?style=flat&logo=react&logoColor=white)](https://react.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 🎯 Why This Project Matters

Cloud scheduling is a billion-dollar problem. Traditional rule-based schedulers fail to balance cost, performance, and sustainability simultaneously. **CloudOS-RL** replaces static heuristics with a trained PPO agent that makes adaptive, explainable decisions in under 700ms — bridging the gap between academic RL research and production-grade MLOps.

---

## ✨ Key Features

| Feature | Details |
|---|---|
| 🤖 **RL-Powered Scheduling** | PPO agent (Stable-Baselines3) trained on multi-objective reward: cost + latency + carbon + SLA |
| 🔍 **Explainable AI** | SHAP values expose per-decision feature attribution — no black box |
| ⚡ **Real-Time Streaming** | Kafka event bus delivers scheduling decisions to the UI with sub-second latency |
| ☸️ **Kubernetes-Native** | Fully containerized; deploys to Minikube or any K8s cluster out of the box |
| 🌐 **Multi-Cloud Aware** | Abstracts AWS / GCP / Azure provider configs into a unified scheduling surface |
| 📊 **Live Dashboard** | React + Vite monitoring UI with scheduling history, SHAP charts, and KPI tiles |

---

## 🏗️ Architecture

```
Client Request
     │
     ▼
 FastAPI Layer          ← REST API + input validation
     │
     ▼
 PPO RL Agent           ← Stable-Baselines3 · multi-objective reward
     │
     ▼
 SHAP Explainer         ← Feature-level decision attribution
     │
     ▼
 Kafka Producer         ← Async event streaming
     │
     ▼
 React Dashboard        ← Real-time UI · scheduling output + SHAP viz
```

---

## 🛠️ Tech Stack

**Backend** · FastAPI · Python 3.10+ · Stable-Baselines3 (PPO) · SHAP · Apache Kafka  
**Frontend** · React · Vite · Recharts  
**Infrastructure** · Docker · Kubernetes · Minikube  

---

## 📸 Screenshots

<table>
  <tr>
    <td><b>Dashboard Overview</b></td>
    <td><b>Scheduling Form</b></td>
  </tr>
  <tr>
    <td><img src="docs/images/dashboard.png" width="400"/></td>
    <td><img src="docs/images/schedule-form.png" width="400"/></td>
  </tr>
  <tr>
    <td><b>Decision Output</b></td>
    <td><b>SHAP Explainability</b></td>
  </tr>
  <tr>
    <td><img src="docs/images/decision-output.png" width="400"/></td>
    <td><img src="docs/images/shap-explain.png" width="400"/></td>
  </tr>
</table>

---

## ⚡ Performance

- **Scheduling Latency:** 300–700 ms end-to-end
- **Load Stability:** Consistent throughput under concurrent requests
- **Kafka Throughput:** Real-time event delivery with no observable UI lag

---

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/KadhirDev/CloudOS-RL.git  
cd cloudos-rl

# 2. Start services
docker-compose up --build

# 3. Deploy to Kubernetes (optional)
kubectl apply -f k8s/

# 4. Open dashboard
open http://localhost:5173
```

---

## 📁 Project Structure

```
cloudos-rl/
├── api/              # FastAPI app · routes · request models
├── agent/            # PPO training · environment · reward shaping
├── explainer/        # SHAP integration · attribution pipeline
├── streaming/        # Kafka producer · consumer setup
├── dashboard/        # React + Vite frontend
├── k8s/              # Kubernetes manifests
└── docker-compose.yml
```

---

## 🤝 Contributing

PRs welcome. Open an issue first for major changes.

---

> Built to demonstrate end-to-end ML system design: from RL training to real-time inference, explainability, and cloud-native deployment.
