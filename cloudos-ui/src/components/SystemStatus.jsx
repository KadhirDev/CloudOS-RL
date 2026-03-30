import { useEffect, useState } from 'react'
import { getStatus } from '../api/client'

export default function SystemStatus() {
  const [s, setS] = useState(null)

  useEffect(() => {
    const load = async () => {
      try { setS(await getStatus()) } catch {}
    }
    load()
    const t = setInterval(load, 8000)
    return () => clearInterval(t)
  }, [])

  if (!s) return null

  const items = [
    { label: 'RL Agent',    ok: s.agent_loaded },
    { label: 'SHAP',        ok: s.shap_ready   },
    { label: 'Decisions',   ok: true, value: s.decisions_served },
  ]

  return (
    <div style={{
      display: 'flex', gap: 16, padding: '8px 16px',
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 8, fontSize: 11, alignItems: 'center',
      marginBottom: 20,
    }}>
      <span style={{ color: 'var(--muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
        System
      </span>
      {items.map(({ label, ok, value }) => (
        <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
          <div style={{
            width: 7, height: 7, borderRadius: '50%',
            background: ok ? 'var(--green)' : 'var(--red)',
            boxShadow: ok ? '0 0 6px var(--green)' : 'none',
          }} />
          <span style={{ color: 'var(--text2)' }}>
            {label}{value != null ? `: ${value}` : ''}
          </span>
        </div>
      ))}
    </div>
  )
}