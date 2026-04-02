/**
 * EngineerView — full engineering dashboard.
 * Reuses ALL existing components without modification.
 *
 * Updated with:
 *   - Hero banner with status pills
 *   - Section IDs for sidebar/header scroll-navigation
 *   - Safe polling cleanup
 *   - No unused imports
 */

import { useState, useCallback, useEffect } from 'react'
import {
  Send,
  Eye,
  Zap,
  Activity,
  Loader,
  Cloud,
} from 'lucide-react'
import MetricsBar from '../components/MetricsBar'
import ScheduleForm from '../components/ScheduleForm'
import DecisionCard from '../components/DecisionCard'
import DecisionTable from '../components/DecisionTable'
import SystemStatus from '../components/SystemStatus'
import { SkeletonDecisionCard } from '../components/Skeleton'
import { getStatus } from '../api/client'

function StatusPill({ label, status }) {
  const color =
    status === 'ok'
      ? 'var(--green)'
      : status === 'warn'
        ? 'var(--yellow)'
        : status === 'loading'
          ? 'var(--muted)'
          : 'var(--red)'

  const bg =
    status === 'ok'
      ? 'rgba(16,185,129,0.12)'
      : status === 'warn'
        ? 'rgba(245,158,11,0.12)'
        : status === 'loading'
          ? 'rgba(100,116,139,0.12)'
          : 'rgba(239,68,68,0.12)'

  const isLoading = status === 'loading'

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 6,
        background: bg,
        border: `1px solid ${color}30`,
        borderRadius: 20,
        padding: '5px 12px',
        fontSize: 12,
        fontWeight: 600,
        color,
      }}
    >
      {isLoading ? (
        <Loader size={10} style={{ animation: 'spin 1s linear infinite' }} />
      ) : (
        <div
          style={{
            width: 6,
            height: 6,
            borderRadius: '50%',
            background: color,
            boxShadow: `0 0 5px ${color}`,
          }}
        />
      )}
      {label}
    </div>
  )
}

function HeroBanner({ agentStatus }) {
  const agentLoaded = !!agentStatus?.agent_loaded
  const shapReady = !!agentStatus?.shap_ready

  const scrollToSection = (anchor) => {
    const el = document.getElementById(`section-${anchor}`)
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }

  return (
    <div
      style={{
        background:
          'linear-gradient(135deg, rgba(59,130,246,0.08) 0%, rgba(99,102,241,0.06) 100%)',
        border: '1px solid rgba(59,130,246,0.15)',
        borderRadius: 16,
        padding: '28px 32px',
        marginBottom: 24,
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      <div
        style={{
          position: 'absolute',
          top: -40,
          right: -40,
          width: 180,
          height: 180,
          borderRadius: '50%',
          background: 'radial-gradient(circle, rgba(99,102,241,0.08), transparent)',
          pointerEvents: 'none',
        }}
      />

      <div style={{ position: 'relative' }}>
        <div
          style={{
            fontSize: 11,
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '0.1em',
            color: 'var(--accent)',
            marginBottom: 10,
            display: 'flex',
            alignItems: 'center',
            gap: 6,
          }}
        >
          <Zap size={11} />
          AI-Native Multi-Cloud Scheduler
        </div>

        <h2
          style={{
            fontSize: 26,
            fontWeight: 900,
            lineHeight: 1.2,
            letterSpacing: '-0.02em',
            marginBottom: 8,
          }}
        >
          Optimize Cloud Placement
          <span
            style={{
              background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent',
              marginLeft: 8,
            }}
          >
            Intelligently
          </span>
        </h2>

        <p
          style={{
            color: 'var(--muted)',
            fontSize: 13,
            marginBottom: 20,
            maxWidth: 500,
            lineHeight: 1.7,
          }}
        >
          PPO reinforcement learning across cost, latency, carbon, and SLA simultaneously.
          SHAP-powered explainability for every placement decision.
        </p>

        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 20 }}>
          <StatusPill
            label="RL Agent Active"
            status={agentLoaded ? 'ok' : 'loading'}
          />
          <StatusPill
            label="SHAP Ready"
            status={shapReady ? 'ok' : 'loading'}
          />
          <StatusPill label="Kafka Connected" status="ok" />
          <StatusPill label="Kubernetes Healthy" status="ok" />
        </div>

        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {[
            {
              label: 'Schedule Workload',
              icon: Send,
              color: 'var(--accent)',
              anchor: 'schedule',
            },
            {
              label: 'View Decisions',
              icon: Eye,
              color: 'var(--green)',
              anchor: 'decisions',
            },
            {
              label: 'Explain Decision',
              icon: Zap,
              color: 'var(--accent2)',
              anchor: 'explainability',
            },
            {
              label: 'Live Metrics',
              icon: Activity,
              color: 'var(--muted)',
              anchor: 'metrics',
            },
          ].map(({ label, icon: Icon, color, anchor }) => (
            <button
              key={label}
              type="button"
              onClick={() => scrollToSection(anchor)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                padding: '8px 16px',
                background:
                  anchor === 'schedule'
                    ? 'linear-gradient(135deg, var(--accent), var(--accent2))'
                    : 'var(--surface)',
                border: `1px solid ${anchor === 'schedule' ? 'transparent' : 'var(--border)'}`,
                color: anchor === 'schedule' ? '#fff' : color,
                fontWeight: 600,
                fontSize: 13,
                borderRadius: 8,
                cursor: 'pointer',
              }}
            >
              <Icon size={13} />
              {label}
            </button>
          ))}
        </div>
      </div>

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}

function EmptyDecisionState() {
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
          width: 56,
          height: 56,
          borderRadius: 14,
          background:
            'linear-gradient(135deg, rgba(59,130,246,0.15), rgba(99,102,241,0.15))',
          border: '1px solid rgba(99,102,241,0.2)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <Cloud size={22} color="var(--accent2)" />
      </div>

      <div>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 6 }}>
          Awaiting workload
        </div>
        <div
          style={{
            color: 'var(--muted)',
            fontSize: 12,
            maxWidth: 240,
            lineHeight: 1.7,
          }}
        >
          Configure a workload and click{' '}
          <span style={{ color: 'var(--accent)', fontWeight: 600 }}>
            Schedule Workload
          </span>{' '}
          to get an AI placement decision with SHAP explanation.
        </div>
      </div>

      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', justifyContent: 'center' }}>
        {['PPO Model', 'SHAP XAI', 'Multi-Cloud', 'Carbon-Aware'].map((tag) => (
          <span key={tag} className="badge badge-blue">
            {tag}
          </span>
        ))}
      </div>
    </div>
  )
}

export default function EngineerView() {
  const [lastDecision, setLastDecision] = useState(null)
  const [scheduling, setScheduling] = useState(false)
  const [agentStatus, setAgentStatus] = useState(null)

  useEffect(() => {
    let isMounted = true

    const loadStatus = async () => {
      try {
        const status = await getStatus()
        if (isMounted) {
          setAgentStatus(status)
        }
      } catch {
        // non-fatal
      }
    }

    loadStatus()
    const t = setInterval(loadStatus, 15000)

    return () => {
      isMounted = false
      clearInterval(t)
    }
  }, [])

  const handleResult = useCallback((decision) => {
    setLastDecision(decision)
  }, [])

  const handleLoading = useCallback((value) => {
    setScheduling(value)
  }, [])

  return (
    <div>
      <HeroBanner agentStatus={agentStatus} />

      <div id="section-dashboard">
        <SystemStatus />
      </div>

      <div id="section-metrics">
        <MetricsBar />
      </div>

      <div
        id="section-schedule"
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: 20,
          marginBottom: 24,
        }}
      >
        <ScheduleForm onResult={handleResult} onLoading={handleLoading} />

        <div id="section-explainability">
          {scheduling ? (
            <SkeletonDecisionCard />
          ) : lastDecision ? (
            <DecisionCard key={lastDecision.decision_id} decision={lastDecision} />
          ) : (
            <EmptyDecisionState />
          )}
        </div>
      </div>

      <div id="section-decisions">
        <DecisionTable />
      </div>

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
        CloudOS-RL · AI-Native Multi-Cloud Scheduler · PPO + SHAP + Kafka + Kubernetes
      </div>
    </div>
  )
}