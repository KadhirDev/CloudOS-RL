import { useEffect, useState } from 'react'
import { TrendingDown, Leaf, Clock, Cpu, BarChart2 } from 'lucide-react'
import { getDecisions, getStatus } from '../api/client'
import { useAuth } from '../auth/AuthContext'

function KpiCard({ icon: Icon, label, value, sub, color }) {
  return (
    <div className="card card-hover" style={{ flex: 1, display: 'flex', gap: 16, alignItems: 'center' }}>
      <div style={{
        width: 48, height: 48, borderRadius: 12,
        background: `${color}18`, border: `1px solid ${color}30`,
        display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
      }}>
        <Icon size={20} color={color} />
      </div>
      <div>
        <div style={{ fontSize: 11, color: 'var(--muted)', textTransform: 'uppercase', letterSpacing: '0.06em', fontWeight: 700, marginBottom: 3 }}>
          {label}
        </div>
        <div style={{ fontSize: 26, fontWeight: 900, lineHeight: 1, letterSpacing: '-0.02em' }}>{value}</div>
        {sub && <div style={{ color: 'var(--muted)', fontSize: 11, marginTop: 4 }}>{sub}</div>}
      </div>
    </div>
  )
}

export default function ExecutiveView() {
  const { user }       = useAuth()
  const [decisions, setDecisions] = useState([])
  const [status,    setStatus]    = useState(null)

  useEffect(() => {
    const load = async () => {
      try { setStatus(await getStatus()) }    catch {}
      try { const d = await getDecisions(100); setDecisions(d.decisions || []) } catch {}
    }
    load()
    const t = setInterval(load, 15000)
    return () => clearInterval(t)
  }, [])

  const n          = decisions.length
  const avgCost    = n ? (decisions.reduce((s, d) => s + (d.cost_savings_pct    || 0), 0) / n) : 0
  const avgCarbon  = n ? (decisions.reduce((s, d) => s + (d.carbon_savings_pct  || 0), 0) / n) : 0
  const avgLatency = n ? (decisions.reduce((s, d) => s + (d.latency_ms          || 0), 0) / n) : 0

  // Estimate total monthly savings (illustrative)
  const monthlySavingsEst = n > 0
    ? `$${(avgCost / 100 * 0.096 * 730 * n).toFixed(0)}`
    : '—'

  return (
    <div>
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 800, marginBottom: 4 }}>Executive Dashboard</h1>
          <p style={{ color: 'var(--muted)', fontSize: 13 }}>
            CloudOS-RL performance summary · Refreshes every 15s
          </p>
        </div>
        <div style={{
          background: 'var(--surface2)', border: '1px solid var(--border)',
          borderRadius: 8, padding: '8px 14px', fontSize: 12, color: 'var(--muted)',
        }}>
          Welcome, <strong style={{ color: 'var(--text)' }}>{user?.username}</strong>
        </div>
      </div>

      {/* KPI row */}
      <div style={{ display: 'flex', gap: 16, marginBottom: 24 }}>
        <KpiCard icon={Cpu}         label="Decisions Made"      value={status?.decisions_served ?? 0}       sub="total placements"         color="var(--accent)"  />
        <KpiCard icon={TrendingDown} label="Avg Cost Savings"   value={`${avgCost.toFixed(1)}%`}            sub="vs on-demand baseline"    color="var(--green)"   />
        <KpiCard icon={Leaf}        label="Avg Carbon Savings"  value={`${avgCarbon.toFixed(1)}%`}          sub="vs us-east-1 baseline"    color="var(--green2)"  />
        <KpiCard icon={Clock}       label="Avg Latency"         value={`${avgLatency.toFixed(0)}ms`}        sub="scheduling decision time" color="var(--accent2)" />
      </div>

      {/* Summary insights */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 24 }}>
        <div className="card">
          <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 16 }}>
            <BarChart2 size={14} style={{ display: 'inline', marginRight: 6 }} />
            System Status
          </div>
          {[
            { label: 'RL Agent',         value: status?.agent_loaded ? 'Active'   : 'Loading',   ok: status?.agent_loaded    },
            { label: 'SHAP Explainability', value: status?.shap_ready ? 'Ready' : 'Initialising', ok: status?.shap_ready   },
            { label: 'Decisions Served', value: String(status?.decisions_served ?? 0),            ok: true                    },
            { label: 'Last Cloud',       value: status?.last_decision_cloud  || '—',              ok: true                    },
            { label: 'Last Region',      value: status?.last_decision_region || '—',              ok: true                    },
          ].map(({ label, value, ok }) => (
            <div key={label} style={{
              display: 'flex', justifyContent: 'space-between',
              padding: '8px 0', borderBottom: '1px solid var(--border)', fontSize: 13,
            }}>
              <span style={{ color: 'var(--muted)' }}>{label}</span>
              <span style={{ fontWeight: 600, color: ok ? 'var(--text)' : 'var(--yellow)' }}>{value}</span>
            </div>
          ))}
        </div>

        <div className="card">
          <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 16 }}>
            Estimated Impact
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {[
              { label: 'Avg cost reduction per decision',   value: `${avgCost.toFixed(1)}%`    },
              { label: 'Avg carbon reduction per decision', value: `${avgCarbon.toFixed(1)}%`  },
              { label: 'Est. total cost avoided (sample)',  value: monthlySavingsEst            },
              { label: 'AI scheduler status',               value: status?.agent_loaded ? '✅ Operational' : '⚠ Loading' },
            ].map(({ label, value }) => (
              <div key={label} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 12px', background: 'var(--surface2)',
                borderRadius: 8, fontSize: 13,
              }}>
                <span style={{ color: 'var(--muted)' }}>{label}</span>
                <span style={{ fontWeight: 700 }}>{value}</span>
              </div>
            ))}
          </div>
          <p style={{ color: 'var(--muted)', fontSize: 11, marginTop: 12, lineHeight: 1.6 }}>
            * Estimates based on current session decisions. Actual savings depend on workload volume and cloud pricing.
          </p>
        </div>
      </div>
    </div>
  )
}