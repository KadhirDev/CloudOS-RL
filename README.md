CloudOS-RL
AI Multi-Cloud Scheduler

AI-powered cloud placement using Reinforcement Learning, with explainable decisions and real-time streaming.

🚀 Overview

CloudOS-RL automatically selects the best cloud (AWS, Azure, GCP) based on

💰 Cost
⚡ Latency
🌱 Carbon
🛡 SLA

📸 Product  

Dashboard  
![Dashboard](docs/images/dashboard.png)

Scheduling  
![Schedule Form](docs/images/schedule-form.png)

Decision Output  
![Decision Output](docs/images/decision-output.png)

Explainability  
![SHAP](docs/images/shap-explain.png)


🧠 System Flow

Request → FastAPI → PPO RL Engine → Decision
↓
SHAP Explanation
↓
Kafka → Dashboard

🔐 Security

JWT Authentication
bcrypt Password Hashing
Role-Based Access Control

Roles → viewer · user · engineer · admin · executive

🧰 Tech Stack

Backend → FastAPI (Python)
AI → PPO (Stable-Baselines3)
Explainability → SHAP
Streaming → Kafka
Frontend → React + Vite
Infrastructure → Docker + Kubernetes

📡 API

POST /auth/register
POST /auth/login
POST /api/v1/schedule
GET /api/v1/decisions
POST /api/v1/decisions/{id}/explain

⚙️ Run

git clone https://github.com/KadhirDev/CloudOS-RL.git

cd CloudOS-RL

docker build -t cloudos-api .
minikube start
kubectl apply -f infrastructure/k8s/
kubectl port-forward svc/cloudos-api-svc 8001:8000 -n cloudos-rl

📊 Performance

Latency → 300–700 ms
Stable under load
