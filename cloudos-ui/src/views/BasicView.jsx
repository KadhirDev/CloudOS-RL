import { useAuth } from '../auth/AuthContext'
import MetricsBar  from '../components/MetricsBar'

export default function BasicView() {
  const { user } = useAuth()
  return (
    <div>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 20, fontWeight: 800, marginBottom: 4 }}>
          Welcome, {user?.username}
        </h1>
        <p style={{ color: 'var(--muted)', fontSize: 13 }}>
          CloudOS-RL — AI-powered cloud scheduler overview
        </p>
      </div>

      <MetricsBar />

      <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
        <div style={{ fontSize: 36, marginBottom: 16 }}>⚡</div>
        <div style={{ fontWeight: 700, fontSize: 16, marginBottom: 8 }}>
          AI Cloud Scheduling Platform
        </div>
        <p style={{ color: 'var(--muted)', fontSize: 13, maxWidth: 420, margin: '0 auto 16px' }}>
          This platform uses reinforcement learning to optimise multi-cloud workload placement
          for cost, carbon, and latency simultaneously.
        </p>
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', flexWrap: 'wrap' }}>
          {['PPO Reinforcement Learning', 'SHAP Explainability', 'Multi-Cloud', 'Carbon-Aware'].map(tag => (
            <span key={tag} className="badge badge-blue">{tag}</span>
          ))}
        </div>
      </div>
    </div>
  )
}