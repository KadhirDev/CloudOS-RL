import { useState } from 'react'
import { useAuth } from './AuthContext'
import { Zap, Loader } from 'lucide-react'

const DEMO_ACCOUNTS = [
  { username: 'viewer',    password: 'viewer123',  role: 'Viewer',    hint: 'Read-only'          },
  { username: 'alice',     password: 'alice123',   role: 'User',      hint: 'Submit workloads'   },
  { username: 'engineer',  password: 'eng123',     role: 'Engineer',  hint: 'Full engineering'   },
  { username: 'executive', password: 'exec123',    role: 'Executive', hint: 'KPI dashboard'      },
  { username: 'admin',     password: 'admin123',   role: 'Admin',     hint: 'All access'         },
]

export default function LoginPage() {
  const { login }        = useAuth()
  const [form,  setForm] = useState({ username: '', password: '' })
  const [error, setError]= useState(null)
  const [loading, setLoading] = useState(false)

  const submit = async (u = form.username, p = form.password) => {
    setLoading(true); setError(null)
    try {
      await login(u, p)
    } catch (e) {
      setError(e?.response?.data?.detail || 'Login failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{
      minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
      background: 'var(--bg)',
    }}>
      <div style={{ width: 400 }}>
        {/* Logo */}
        <div style={{ textAlign: 'center', marginBottom: 32 }}>
          <div style={{
            width: 52, height: 52, borderRadius: 14, margin: '0 auto 14px',
            background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <Zap size={24} color="#fff" />
          </div>
          <h1 style={{ fontSize: 22, fontWeight: 800, marginBottom: 4 }}>CloudOS-RL</h1>
          <p style={{ color: 'var(--muted)', fontSize: 13 }}>AI-Native Multi-Cloud Scheduler</p>
        </div>

        {/* Form */}
        <div className="card">
          <div style={{ marginBottom: 20 }}>
            <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 4 }}>Sign in</div>
            <div style={{ color: 'var(--muted)', fontSize: 12 }}>
              Use a demo account or your credentials
            </div>
          </div>

          <label>Username</label>
          <input
            style={{ marginBottom: 12 }}
            value={form.username}
            onChange={e => setForm(f => ({ ...f, username: e.target.value }))}
            placeholder="username"
            onKeyDown={e => e.key === 'Enter' && submit()}
          />

          <label>Password</label>
          <input
            type="password"
            style={{ marginBottom: 20 }}
            value={form.password}
            onChange={e => setForm(f => ({ ...f, password: e.target.value }))}
            placeholder="password"
            onKeyDown={e => e.key === 'Enter' && submit()}
          />

          {error && (
            <div style={{
              background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)',
              borderRadius: 8, padding: '8px 12px', color: '#fca5a5',
              fontSize: 12, marginBottom: 16,
            }}>
              {error}
            </div>
          )}

          <button
            onClick={() => submit()}
            disabled={loading || !form.username}
            style={{
              width: '100%', padding: '11px 0',
              background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
              color: '#fff', fontWeight: 700, fontSize: 14,
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
            }}
          >
            {loading ? <><Loader size={14} style={{ animation: 'spin 0.8s linear infinite' }} /> Signing in…</> : 'Sign In'}
          </button>
        </div>

        {/* Demo accounts */}
        <div className="card" style={{ marginTop: 16 }}>
          <div style={{ fontSize: 11, color: 'var(--muted)', marginBottom: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            Demo Accounts
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {DEMO_ACCOUNTS.map(a => (
              <button
                key={a.username}
                onClick={() => submit(a.username, a.password)}
                disabled={loading}
                style={{
                  padding: '8px 12px', background: 'var(--surface2)',
                  border: '1px solid var(--border)', color: 'var(--text)',
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  borderRadius: 8, textAlign: 'left',
                }}
              >
                <div>
                  <span style={{ fontWeight: 600, fontSize: 13 }}>{a.username}</span>
                  <span style={{ color: 'var(--muted)', fontSize: 11, marginLeft: 8 }}>{a.hint}</span>
                </div>
                <span style={{
                  fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
                  letterSpacing: '0.06em', color: 'var(--accent2)',
                }}>
                  {a.role}
                </span>
              </button>
            ))}
          </div>
        </div>
        <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      </div>
    </div>
  )
}