/**
 * ExecutiveView
 * =============
 * Executive KPI dashboard — read-only.
 *
 * SNAPSHOT FIX:
 *   Previously relied on local `lastDecision` state which was always empty
 *   because executives cannot schedule (RBAC enforced).
 *   Now fetches most recent decision directly from GET /api/v1/decisions?limit=1.
 *   Polls every 15 seconds. Purely read-only — RBAC unchanged.
 */

import { useEffect, useState } from 'react'
import {
  TrendingDown,
  Leaf,
  Clock,
  Cpu,
  BarChart2,
  Cloud,
  MapPin,
  Server,
  Tag,
  Activity,
} from 'lucide-react'
import { getDecisions, getStatus } from '../api/client'
import { useAuth } from '../auth/AuthContext'

function KpiCard({ icon: Icon, label, value, sub, color, animate }) {
  return (
    <div
      className="card card-hover fade-in"
      style={{ flex: 1, display: 'flex', gap: 16, alignItems: 'center' }}
    >
      <div
        style={{
          width: 48,
          height: 48,
          borderRadius: 12,
          background: `${color}18`,
          border: `1px solid ${color}30`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexShrink: 0,
        }}
      >
        <Icon size={20} color={color} />
      </div>
      <div>
        <div
          style={{
            fontSize: 11,
            color: 'var(--muted)',
            textTransform: 'uppercase',
            letterSpacing: '0.06em',
            fontWeight: 700,
            marginBottom: 3,
          }}
        >
          {label}
        </div>
        <div
          style={{
            fontSize: 26,
            fontWeight: 900,
            lineHeight: 1,
            letterSpacing: '-0.02em',
            color: animate ? color : 'var(--text)',
          }}
        >
          {value}
        </div>
        {sub && (
          <div style={{ color: 'var(--muted)', fontSize: 11, marginTop: 4 }}>
            {sub}
          </div>
        )}
      </div>
    </div>
  )
}

function DecisionSnapshotCard({ decision }) {
  if (!decision) {
    return (
      <div
        className="card"
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          minHeight: 180,
          gap: 10,
          color: 'var(--muted)',
        }}
      >
        <Activity size={24} />
        <div style={{ fontSize: 13, fontWeight: 600 }}>No decisions yet</div>
        <div style={{ fontSize: 12 }}>
          Decisions will appear here once the system processes workloads
        </div>
      </div>
    )
  }

  const CLOUD_COLORS = {
    aws: '#f59e0b',
    gcp: '#3b82f6',
    azure: '#6366f1',
    hybrid: '#10b981',
  }

  const cloudColor = CLOUD_COLORS[decision.cloud] || 'var(--accent)'

  const latencyText =
    typeof decision.latency_ms === 'number'
      ? `${decision.latency_ms.toFixed(0)}ms`
      : '—'

  const costText =
    typeof decision.estimated_cost_per_hr === 'number'
      ? `$${decision.estimated_cost_per_hr.toFixed(4)}`
      : '—'

  const costSavingsText =
    typeof decision.cost_savings_pct === 'number'
      ? `${decision.cost_savings_pct.toFixed(1)}%`
      : '—'

  const carbonSavingsText =
    typeof decision.carbon_savings_pct === 'number'
      ? `${decision.carbon_savings_pct.toFixed(1)}%`
      : '—'

  return (
    <div className="card fade-in" style={{ borderLeft: `3px solid ${cloudColor}` }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 14 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <span
              style={{
                fontWeight: 700,
                color: cloudColor,
                textTransform: 'uppercase',
                fontSize: 13,
              }}
            >
              {decision.cloud || '—'}
            </span>
            <span className="badge badge-green">
              {decision.purchase_option?.replace(/_/g, ' ') || '—'}
            </span>
          </div>
          <div style={{ color: 'var(--muted)', fontSize: 11, fontFamily: 'monospace' }}>
            {decision.decision_id ? `${decision.decision_id.slice(0, 16)}…` : '—'}
          </div>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div
            style={{
              fontSize: 10,
              color: 'var(--muted)',
              textTransform: 'uppercase',
              letterSpacing: '0.06em',
            }}
          >
            Latency
          </div>
          <div style={{ fontSize: 20, fontWeight: 800, color: 'var(--green)' }}>
            {latencyText}
          </div>
        </div>
      </div>

      {[
        { icon: MapPin, label: 'Region', value: decision.region || '—' },
        { icon: Server, label: 'Instance', value: decision.instance_type || '—' },
        { icon: Tag, label: 'Est. Cost/hr', value: costText },
      ].map(({ icon: Icon, label, value }) => (
        <div
          key={label}
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            padding: '7px 0',
            borderBottom: '1px solid var(--border)',
            fontSize: 12,
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, color: 'var(--muted)' }}>
            <Icon size={11} />
            {label}
          </div>
          <span style={{ fontWeight: 600 }}>{value}</span>
        </div>
      ))}

      <div style={{ display: 'flex', gap: 10, marginTop: 12 }}>
        <div
          style={{
            flex: 1,
            textAlign: 'center',
            padding: '8px 0',
            background: 'rgba(16,185,129,0.08)',
            borderRadius: 8,
            border: '1px solid rgba(16,185,129,0.2)',
          }}
        >
          <div style={{ fontSize: 10, color: 'var(--muted)', marginBottom: 2 }}>
            Cost Savings
          </div>
          <div style={{ fontWeight: 900, color: 'var(--green)', fontSize: 18 }}>
            {costSavingsText}
          </div>
        </div>
        <div
          style={{
            flex: 1,
            textAlign: 'center',
            padding: '8px 0',
            background: 'rgba(52,211,153,0.08)',
            borderRadius: 8,
            border: '1px solid rgba(52,211,153,0.2)',
          }}
        >
          <div style={{ fontSize: 10, color: 'var(--muted)', marginBottom: 2 }}>
            Carbon Savings
          </div>
          <div style={{ fontWeight: 900, color: 'var(--green2)', fontSize: 18 }}>
            {carbonSavingsText}
          </div>
        </div>
      </div>
    </div>
  )
}

export default function ExecutiveView() {
  const { user } = useAuth()
  const [decisions, setDecisions] = useState([])
  const [status, setStatus] = useState(null)
  const [latestDecision, setLatestDecision] = useState(null)

  useEffect(() => {
    let isMounted = true

    const load = async () => {
      try {
        const nextStatus = await getStatus()
        if (isMounted) setStatus(nextStatus)
      } catch {
        // non-fatal for executive dashboard
      }

      try {
        const d = await getDecisions(100)
        const list = d?.decisions || []

        if (!isMounted) return

        setDecisions(list)

        // SNAPSHOT FIX:
        // Executive cannot schedule (RBAC), so local session state is always empty.
        // Use latest decision from API data instead.
        setLatestDecision(list.length > 0 ? list[0] : null)
      } catch {
        if (isMounted) {
          setDecisions([])
          setLatestDecision(null)
        }
      }
    }

    load()
    const t = setInterval(load, 15000)

    return () => {
      isMounted = false
      clearInterval(t)
    }
  }, [])

  const n = decisions.length
  const avgCost = n
    ? decisions.reduce((s, d) => s + (d.cost_savings_pct || 0), 0) / n
    : 0
  const avgCarbon = n
    ? decisions.reduce((s, d) => s + (d.carbon_savings_pct || 0), 0) / n
    : 0
  const avgLatency = n
    ? decisions.reduce((s, d) => s + (d.latency_ms || 0), 0) / n
    : 0

  // Preserved useful metric from the older version.
  // Illustrative estimate only — intentionally conservative and read-only.
  const monthlySavingsEst =
    n > 0 ? `$${((avgCost / 100) * 0.096 * 730 * n).toFixed(0)}` : '—'

  return (
    <div>
      {/* Page header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: 24,
        }}
      >
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 800, marginBottom: 4 }}>
            Executive Dashboard
          </h1>
          <p style={{ color: 'var(--muted)', fontSize: 13 }}>
            CloudOS-RL performance summary · Auto-refresh every 15s
          </p>
        </div>
        <div
          style={{
            background: 'var(--surface2)',
            border: '1px solid var(--border)',
            borderRadius: 8,
            padding: '8px 14px',
            fontSize: 12,
            color: 'var(--muted)',
          }}
        >
          Welcome, <strong style={{ color: 'var(--text)' }}>{user?.username}</strong>
          <span
            style={{
              marginLeft: 8,
              color: 'var(--accent2)',
              fontWeight: 700,
              fontSize: 10,
              textTransform: 'uppercase',
            }}
          >
            {user?.role}
          </span>
        </div>
      </div>

      {/* KPI row */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 24 }}>
        <KpiCard
          icon={Cpu}
          label="Total Decisions"
          value={status?.decisions_served ?? 0}
          sub="AI placements made"
          color="var(--accent)"
        />
        <KpiCard
          icon={TrendingDown}
          label="Avg Cost Savings"
          value={`${avgCost.toFixed(1)}%`}
          sub="vs on-demand baseline"
          color="var(--green)"
          animate
        />
        <KpiCard
          icon={Leaf}
          label="Avg Carbon Savings"
          value={`${avgCarbon.toFixed(1)}%`}
          sub="vs us-east-1 baseline"
          color="var(--green2)"
          animate
        />
        <KpiCard
          icon={Clock}
          label="Avg Latency"
          value={`${avgLatency.toFixed(0)}ms`}
          sub="scheduling decision time"
          color="var(--accent2)"
        />
      </div>

      {/* Two-column: status + impact */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: 20,
          marginBottom: 24,
        }}
      >
        {/* System status */}
        <div className="card">
          <div
            style={{
              fontWeight: 700,
              fontSize: 14,
              marginBottom: 16,
              display: 'flex',
              alignItems: 'center',
              gap: 8,
            }}
          >
            <BarChart2 size={14} />
            System Status
          </div>
          {[
            {
              label: 'RL Agent',
              value: status?.agent_loaded ? '✅ Active' : '⚠ Loading',
            },
            {
              label: 'SHAP Explainability',
              value: status?.shap_ready ? '✅ Ready' : '⏳ Initialising',
            },
            {
              label: 'Decisions Served',
              value: String(status?.decisions_served ?? 0),
            },
            {
              label: 'Last Cloud',
              value: status?.last_decision_cloud || '—',
            },
            {
              label: 'Last Region',
              value: status?.last_decision_region || '—',
            },
          ].map(({ label, value }) => (
            <div
              key={label}
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                padding: '8px 0',
                borderBottom: '1px solid var(--border)',
                fontSize: 13,
              }}
            >
              <span style={{ color: 'var(--muted)' }}>{label}</span>
              <span style={{ fontWeight: 600 }}>{value}</span>
            </div>
          ))}
        </div>

        {/* Estimated impact */}
        <div className="card">
          <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 16 }}>
            Estimated Business Impact
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {[
              {
                label: 'Avg cost reduction per decision',
                value: `${avgCost.toFixed(1)}%`,
              },
              {
                label: 'Avg carbon reduction per decision',
                value: `${avgCarbon.toFixed(1)}%`,
              },
              {
                label: 'Avg scheduling latency',
                value: `${avgLatency.toFixed(0)}ms`,
              },
              {
                label: 'Est. total cost avoided (sample)',
                value: monthlySavingsEst,
              },
              {
                label: 'AI scheduler',
                value: status?.agent_loaded ? '✅ Operational' : '⚠ Initialising',
              },
            ].map(({ label, value }) => (
              <div
                key={label}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '9px 12px',
                  background: 'var(--surface2)',
                  borderRadius: 8,
                  fontSize: 13,
                }}
              >
                <span style={{ color: 'var(--muted)' }}>{label}</span>
                <span style={{ fontWeight: 700 }}>{value}</span>
              </div>
            ))}
          </div>
          <p style={{ color: 'var(--muted)', fontSize: 11, marginTop: 12, lineHeight: 1.6 }}>
            * Estimates based on current session decisions. Actual savings depend on
            workload volume and cloud pricing.
          </p>
        </div>
      </div>

      {/* Latest decision snapshot */}
      <div>
        <div
          style={{
            fontWeight: 700,
            fontSize: 14,
            marginBottom: 14,
            display: 'flex',
            alignItems: 'center',
            gap: 8,
          }}
        >
          <Cloud size={14} />
          Latest Decision Snapshot
          <span style={{ color: 'var(--muted)', fontWeight: 400, fontSize: 12 }}>
            · read-only
          </span>
        </div>
        <DecisionSnapshotCard decision={latestDecision} />
      </div>
    </div>
  )
}