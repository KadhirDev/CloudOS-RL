import { useState, useCallback } from 'react'
import Layout from './components/Layout'
import MetricsBar from './components/MetricsBar'
import ScheduleForm from './components/ScheduleForm'
import DecisionCard from './components/DecisionCard'
import SystemStatus from './components/SystemStatus'
import DecisionTable from './components/DecisionTable'
import ToastContainer from './components/Toast'
import { SkeletonDecisionCard } from './components/Skeleton'

function EmptyState() {
  return (
    <div
      className="card"
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: 380,
        gap: 14,
        textAlign: 'center',
      }}
    >
      <div
        style={{
          width: 64,
          height: 64,
          borderRadius: 16,
          background:
            'linear-gradient(135deg, rgba(59,130,246,0.15), rgba(99,102,241,0.15))',
          border: '1px solid rgba(99,102,241,0.2)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: 26,
        }}
      >
        ⚡
      </div>

      <div>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 6 }}>
          Awaiting workload
        </div>
        <div
          style={{
            color: 'var(--muted)',
            fontSize: 12,
            maxWidth: 220,
            lineHeight: 1.7,
          }}
        >
          Configure a workload on the left and click{' '}
          <span style={{ color: 'var(--accent)', fontWeight: 600 }}>
            Schedule Workload
          </span>{' '}
          to get an AI placement decision.
        </div>
      </div>

      <div
        style={{
          display: 'flex',
          gap: 8,
          marginTop: 4,
          flexWrap: 'wrap',
          justifyContent: 'center',
        }}
      >
        {['PPO Model', 'SHAP XAI', 'Multi-Cloud'].map((tag) => (
          <span key={tag} className="badge badge-blue">
            {tag}
          </span>
        ))}
      </div>
    </div>
  )
}

export default function App() {
  const [lastDecision, setLastDecision] = useState(null)
  const [scheduling, setScheduling] = useState(false)

  const handleResult = useCallback((decision) => {
    setLastDecision(decision)
  }, [])

  const handleLoading = useCallback((isLoading) => {
    setScheduling(isLoading)
  }, [])

  return (
    <>
      <Layout>
        <div style={{ marginBottom: 26 }}>
          <div
            style={{
              display: 'flex',
              alignItems: 'baseline',
              gap: 12,
              marginBottom: 5,
              flexWrap: 'wrap',
            }}
          >
            <h1
              style={{
                fontSize: 22,
                fontWeight: 800,
                letterSpacing: '-0.02em',
              }}
            >
              AI Cloud Scheduler
            </h1>

            <span className="badge badge-green" style={{ fontSize: 10 }}>
              Live
            </span>
          </div>

          <p style={{ color: 'var(--muted)', fontSize: 13, lineHeight: 1.6 }}>
            Real-time multi-cloud workload placement powered by Proximal Policy
            Optimization (PPO) · SHAP explainability · Carbon-aware scheduling
          </p>
        </div>

        <MetricsBar />
        <SystemStatus />

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '1fr 1fr',
            gap: 20,
            marginBottom: 24,
            alignItems: 'start',
          }}
        >
          <ScheduleForm onResult={handleResult} onLoading={handleLoading} />

          {scheduling ? (
            <SkeletonDecisionCard />
          ) : lastDecision ? (
            <DecisionCard
              key={lastDecision?.decision_id || 'latest-decision'}
              decision={lastDecision}
            />
          ) : (
            <EmptyState />
          )}
        </div>

        <DecisionTable />

        <div
          style={{
            marginTop: 32,
            textAlign: 'center',
            color: 'var(--muted)',
            fontSize: 11,
            borderTop: '1px solid var(--border)',
            paddingTop: 20,
          }}
        >
          CloudOS-RL · AI-Native Multi-Cloud Scheduler · PPO + SHAP + Kafka +
          Kubernetes
        </div>
      </Layout>

      <ToastContainer />
    </>
  )
}