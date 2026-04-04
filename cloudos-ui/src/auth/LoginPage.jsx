import { useEffect, useRef, useState } from 'react'
import { useAuth } from './AuthContext'
import { register as apiRegister } from '../api/client'
import {
  Zap,
  Loader,
  Eye,
  EyeOff,
  ArrowRight,
  UserPlus,
  LogIn,
} from 'lucide-react'

function ParticleCanvas() {
  const canvasRef = useRef(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return

    const ctx = canvas.getContext('2d')
    if (!ctx) return

    let animationFrameId = 0

    const resize = () => {
      canvas.width = window.innerWidth
      canvas.height = window.innerHeight
    }

    resize()
    window.addEventListener('resize', resize)

    const particleCount = 38
    const particles = Array.from({ length: particleCount }, () => ({
      x: Math.random() * canvas.width,
      y: Math.random() * canvas.height,
      r: Math.random() * 1.6 + 0.4,
      vx: (Math.random() - 0.5) * 0.35,
      vy: (Math.random() - 0.5) * 0.35,
      alpha: Math.random() * 0.4 + 0.1,
    }))

    const draw = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height)

      for (let i = 0; i < particles.length; i += 1) {
        for (let j = i + 1; j < particles.length; j += 1) {
          const dx = particles[i].x - particles[j].x
          const dy = particles[i].y - particles[j].y
          const dist = Math.sqrt(dx * dx + dy * dy)

          if (dist < 120) {
            ctx.beginPath()
            ctx.strokeStyle = `rgba(99,102,241,${0.12 * (1 - dist / 120)})`
            ctx.lineWidth = 0.6
            ctx.moveTo(particles[i].x, particles[i].y)
            ctx.lineTo(particles[j].x, particles[j].y)
            ctx.stroke()
          }
        }
      }

      particles.forEach((p) => {
        ctx.beginPath()
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2)
        ctx.fillStyle = `rgba(139,92,246,${p.alpha})`
        ctx.fill()

        p.x += p.vx
        p.y += p.vy

        if (p.x < 0) p.x = canvas.width
        if (p.x > canvas.width) p.x = 0
        if (p.y < 0) p.y = canvas.height
        if (p.y > canvas.height) p.y = 0
      })

      animationFrameId = window.requestAnimationFrame(draw)
    }

    draw()

    return () => {
      window.cancelAnimationFrame(animationFrameId)
      window.removeEventListener('resize', resize)
    }
  }, [])

  return (
    <canvas
      ref={canvasRef}
      aria-hidden="true"
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 0,
        pointerEvents: 'none',
        opacity: 0.7,
      }}
    />
  )
}

export default function LoginPage() {
  const { login } = useAuth()

  const [mode, setMode] = useState('signin')
  const [form, setForm] = useState({
    username: '',
    password: '',
    confirm: '',
  })
  const [showPassword, setShowPassword] = useState(false)
  const [showConfirm, setShowConfirm] = useState(false)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(null)
  const [loading, setLoading] = useState(false)

  const updateField = (key, value) => {
    setForm((prev) => ({ ...prev, [key]: value }))
    setError(null)
    setSuccess(null)
  }

  const switchMode = (nextMode) => {
    if (loading) return
    setMode(nextMode)
    setForm({ username: '', password: '', confirm: '' })
    setError(null)
    setSuccess(null)
    setShowPassword(false)
    setShowConfirm(false)
  }

  const handleSignIn = async () => {
    const username = form.username.trim()
    const password = form.password

    if (!username || !password) {
      setError('Please enter your username and password.')
      return
    }

    setLoading(true)
    setError(null)
    setSuccess(null)

    try {
      await login(username, password)
    } catch (e) {
      setError(e?.response?.data?.detail || 'Invalid username or password.')
    } finally {
      setLoading(false)
    }
  }

  const handleSignUp = async () => {
    const username = form.username.trim()
    const password = form.password
    const confirm = form.confirm

    if (!username) {
      setError('Username is required.')
      return
    }

    if (username.length < 3) {
      setError('Username must be at least 3 characters.')
      return
    }

    if (password.length < 6) {
      setError('Password must be at least 6 characters.')
      return
    }

    if (password !== confirm) {
      setError('Passwords do not match.')
      return
    }

    setLoading(true)
    setError(null)
    setSuccess(null)

    try {
      const result = await apiRegister(username, password, confirm)

      setMode('signin')
      setForm({
        username: result?.username || username,
        password: '',
        confirm: '',
      })
      setShowPassword(false)
      setShowConfirm(false)
      setError(null)
      setSuccess(
        result?.message || `Account created successfully. Sign in as "${result?.username || username}".`
      )
    } catch (e) {
      setError(e?.response?.data?.detail || 'Registration failed. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  const handleSubmit = async () => {
    if (mode === 'signin') {
      await handleSignIn()
      return
    }
    await handleSignUp()
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleSubmit()
    }
  }

  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'var(--bg)',
        position: 'relative',
        overflow: 'hidden',
        padding: '24px',
      }}
    >
      <ParticleCanvas />

      <div
        aria-hidden="true"
        style={{
          position: 'absolute',
          width: 600,
          height: 600,
          borderRadius: '50%',
          background:
            'radial-gradient(circle, rgba(99,102,241,0.07) 0%, transparent 70%)',
          pointerEvents: 'none',
          zIndex: 1,
        }}
      />

      <div
        className="login-card"
        style={{
          width: '100%',
          maxWidth: 420,
          zIndex: 2,
          position: 'relative',
        }}
      >
        <div style={{ textAlign: 'center', marginBottom: 32 }}>
          <div
            style={{
              width: 56,
              height: 56,
              borderRadius: 16,
              margin: '0 auto 14px',
              background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              boxShadow: '0 8px 32px rgba(99,102,241,0.35)',
            }}
          >
            <Zap size={26} color="#fff" />
          </div>

          <h1
            style={{
              fontSize: 22,
              fontWeight: 800,
              marginBottom: 4,
              letterSpacing: '-0.02em',
            }}
          >
            CloudOS-RL
          </h1>

          <p style={{ color: 'var(--muted)', fontSize: 13 }}>
            AI-Native Multi-Cloud Scheduler
          </p>
        </div>

        <div
          style={{
            display: 'flex',
            background: 'var(--surface2)',
            border: '1px solid var(--border)',
            borderRadius: 10,
            padding: 4,
            marginBottom: 24,
            gap: 4,
          }}
        >
          {[
            { id: 'signin', label: 'Sign In', Icon: LogIn },
            { id: 'signup', label: 'Sign Up', Icon: UserPlus },
          ].map(({ id, label, Icon }) => (
            <button
              key={id}
              type="button"
              onClick={() => switchMode(id)}
              disabled={loading}
              style={{
                flex: 1,
                padding: '9px 0',
                borderRadius: 8,
                fontWeight: 600,
                fontSize: 13,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 6,
                background:
                  mode === id
                    ? 'linear-gradient(135deg, var(--accent), var(--accent2))'
                    : 'transparent',
                color: mode === id ? '#fff' : 'var(--muted)',
                border: 'none',
                transition: 'all 0.18s ease',
                opacity: loading ? 0.85 : 1,
              }}
            >
              <Icon size={13} />
              {label}
            </button>
          ))}
        </div>

        <div className="card" style={{ padding: '28px 28px 24px' }}>
          <div style={{ marginBottom: 20 }}>
            <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 4 }}>
              {mode === 'signin' ? 'Welcome back' : 'Create account'}
            </div>
            <div style={{ color: 'var(--muted)', fontSize: 12 }}>
              {mode === 'signin'
                ? 'Sign in with your credentials'
                : 'Create a new account to access CloudOS-RL'}
            </div>
          </div>

          {success && (
            <div
              style={{
                background: 'rgba(16,185,129,0.1)',
                border: '1px solid rgba(16,185,129,0.3)',
                borderRadius: 8,
                padding: '10px 14px',
                color: 'var(--green, #34d399)',
                fontSize: 13,
                marginBottom: 18,
                display: 'flex',
                alignItems: 'center',
                gap: 8,
              }}
            >
              <span aria-hidden="true">✓</span>
              <span>{success}</span>
            </div>
          )}

          <div style={{ marginBottom: 16 }}>
            <label>Username</label>
            <input
              value={form.username}
              onChange={(e) => updateField('username', e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Enter your username"
              autoFocus
              autoComplete="username"
            />
          </div>

          <div
            style={{
              marginBottom: mode === 'signup' ? 16 : 22,
              position: 'relative',
            }}
          >
            <label>Password</label>
            <div style={{ position: 'relative' }}>
              <input
                type={showPassword ? 'text' : 'password'}
                value={form.password}
                onChange={(e) => updateField('password', e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Enter your password"
                autoComplete={mode === 'signin' ? 'current-password' : 'new-password'}
                style={{ paddingRight: 42 }}
              />
              <button
                type="button"
                onClick={() => setShowPassword((prev) => !prev)}
                aria-label={showPassword ? 'Hide password' : 'Show password'}
                style={{
                  position: 'absolute',
                  right: 12,
                  top: '50%',
                  transform: 'translateY(-50%)',
                  background: 'none',
                  color: 'var(--muted)',
                  padding: 0,
                  border: 'none',
                  borderRadius: 0,
                  display: 'flex',
                  alignItems: 'center',
                }}
              >
                {showPassword ? <EyeOff size={15} /> : <Eye size={15} />}
              </button>
            </div>
          </div>

          {mode === 'signup' && (
            <div style={{ marginBottom: 22, position: 'relative' }}>
              <label>Confirm Password</label>
              <div style={{ position: 'relative' }}>
                <input
                  type={showConfirm ? 'text' : 'password'}
                  value={form.confirm}
                  onChange={(e) => updateField('confirm', e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder="Repeat your password"
                  autoComplete="new-password"
                  style={{ paddingRight: 42 }}
                />
                <button
                  type="button"
                  onClick={() => setShowConfirm((prev) => !prev)}
                  aria-label={showConfirm ? 'Hide confirm password' : 'Show confirm password'}
                  style={{
                    position: 'absolute',
                    right: 12,
                    top: '50%',
                    transform: 'translateY(-50%)',
                    background: 'none',
                    color: 'var(--muted)',
                    padding: 0,
                    border: 'none',
                    borderRadius: 0,
                    display: 'flex',
                    alignItems: 'center',
                  }}
                >
                  {showConfirm ? <EyeOff size={15} /> : <Eye size={15} />}
                </button>
              </div>
            </div>
          )}

          {error && (
            <div
              style={{
                background: 'rgba(239,68,68,0.1)',
                border: '1px solid rgba(239,68,68,0.3)',
                borderRadius: 8,
                padding: '10px 14px',
                color: '#fca5a5',
                fontSize: 12,
                marginBottom: 18,
                display: 'flex',
                alignItems: 'flex-start',
                gap: 8,
              }}
            >
              <span style={{ flexShrink: 0, marginTop: 1 }} aria-hidden="true">
                ⚠
              </span>
              <span>{error}</span>
            </div>
          )}

          <button
            type="button"
            onClick={handleSubmit}
            disabled={loading}
            className="login-btn"
            style={{
              width: '100%',
              padding: '12px 0',
              background: loading
                ? 'var(--surface2)'
                : 'linear-gradient(135deg, var(--accent), var(--accent2))',
              color: loading ? 'var(--muted)' : '#fff',
              fontWeight: 700,
              fontSize: 14,
              border: loading ? '1px solid var(--border)' : 'none',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 8,
              letterSpacing: '0.02em',
              borderRadius: 10,
            }}
          >
            {loading ? (
              <>
                <Loader size={15} style={{ animation: 'spin 0.8s linear infinite' }} />
                {mode === 'signin' ? 'Signing in…' : 'Creating account…'}
              </>
            ) : (
              <>
                {mode === 'signin' ? <LogIn size={15} /> : <UserPlus size={15} />}
                {mode === 'signin' ? 'Sign In' : 'Create Account'}
                <ArrowRight size={14} />
              </>
            )}
          </button>
        </div>

        <div
          style={{
            textAlign: 'center',
            marginTop: 20,
            fontSize: 11,
            color: 'var(--muted)',
            lineHeight: 1.6,
          }}
        >
          CloudOS-RL · AI-Native Multi-Cloud Scheduler
          <br />
          PPO · SHAP · Kafka · Kubernetes
        </div>
      </div>

      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }

        @keyframes fadeInUp {
          from { opacity: 0; transform: translateY(20px); }
          to { opacity: 1; transform: none; }
        }

        .login-card {
          animation: fadeInUp 0.4s cubic-bezier(0.16, 1, 0.3, 1) both;
        }

        .login-btn {
          transition: transform 0.12s ease, box-shadow 0.12s ease, opacity 0.12s ease;
        }

        .login-btn:not(:disabled):hover {
          transform: translateY(-1px);
          box-shadow: 0 8px 24px rgba(99,102,241,0.35);
        }

        .login-btn:not(:disabled):active {
          transform: scale(0.98);
          box-shadow: none;
        }
      `}</style>
    </div>
  )
}