/**
 * EngineerView — the original full dashboard, reorganized with role awareness.
 * Reuses ALL existing components without modification.
 */

import { useState, useCallback } from 'react'
import MetricsBar     from '../components/MetricsBar'
import ScheduleForm   from '../components/ScheduleForm'
import DecisionCard   from '../components/DecisionCard'
import DecisionTable  from '../components/DecisionTable'
import SystemStatus   from '../components/SystemStatus'
import { SkeletonDecisionCard } from '../components/Skeleton'
import { Cloud } from 'lucide-react'

function EmptyState() {
  return (
    <div className="card" style={{
      display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center',
      minHeight: 380, gap: 14, textAlign: 'center',
    }}>
      <div style={{
        width: 56, height: 56, borderRadius: 14,
        background: 'linear-gradient(135deg, rgba(59,130,246,0.15), rgba(99,102,241,0.15))',
        border: '1px solid rgba(99,102,241,0.2)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>
        <Cloud size={22} color="var(--accent2)" />
      </div>
      <div>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 6 }}>Awaiting workload</div>
        <div style={{ color: 'var(--muted)', fontSize: 12, maxWidth: 220, lineHeight: 1.7 }}>
          Configure a workload and click{' '}
          <span style={{ color: 'var(--accent)', fontWeight: 600 }}>Schedule Workload</span>{' '}
          to get an AI placement decision.
        </div>
      </div>
    </div>
  )
}

export default function EngineerView() {
  const [lastDecision, setLastDecision] = useState(null)
  const [scheduling,   setScheduling]   = useState(false)

  const handleResult  = useCallback(d  => setLastDecision(d), [])
  const handleLoading = useCallback(v  => setScheduling(v),   [])

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ fontSize: 20, fontWeight: 800, marginBottom: 4 }}>AI Cloud Scheduler</h1>
        <p style={{ color: 'var(--muted)', fontSize: 13 }}>
          Real-time PPO-powered placement · SHAP explainability · Carbon-aware routing
        </p>
      </div>

      <SystemStatus />
      <MetricsBar />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 24 }}>
        <ScheduleForm onResult={handleResult} onLoading={handleLoading} />
        {scheduling
          ? <SkeletonDecisionCard />
          : lastDecision
            ? <DecisionCard key={lastDecision.decision_id} decision={lastDecision} />
            : <EmptyState />
        }
      </div>

      <DecisionTable />
    </div>
  )
}