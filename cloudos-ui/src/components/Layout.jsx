import { Activity, Zap, LogOut, User } from 'lucide-react'

export default function Layout({ children, userInfo, onLogout, headerExtra }) {
  const ROLE_COLORS = {
    viewer: 'var(--muted)',
    user: 'var(--accent)',
    engineer: 'var(--green)',
    admin: 'var(--red)',
    executive: 'var(--accent2)',
  }

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <header
        style={{
          background: 'var(--surface)',
          borderBottom: '1px solid var(--border)',
          padding: '0 32px',
          height: 56,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          position: 'sticky',
          top: 0,
          zIndex: 100,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div
            style={{
              width: 32,
              height: 32,
              borderRadius: 8,
              background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <Zap size={16} color="#fff" />
          </div>

          <span style={{ fontWeight: 700, fontSize: 15 }}>CloudOS</span>

          <span
            style={{
              background: 'var(--surface2)',
              border: '1px solid var(--border)',
              padding: '1px 7px',
              borderRadius: 4,
              fontSize: 10,
              color: 'var(--accent)',
              fontWeight: 700,
              letterSpacing: '0.05em',
            }}
          >
            RL
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {headerExtra}

          {userInfo ? (
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  background: 'var(--surface2)',
                  border: '1px solid var(--border)',
                  borderRadius: 8,
                  padding: '4px 10px',
                  fontSize: 12,
                }}
              >
                <User size={11} color="var(--muted)" />
                <span style={{ color: 'var(--text2)' }}>{userInfo.username}</span>
                <span
                  style={{
                    fontWeight: 700,
                    fontSize: 10,
                    textTransform: 'uppercase',
                    color: ROLE_COLORS[userInfo.role] || 'var(--muted)',
                    letterSpacing: '0.06em',
                  }}
                >
                  {userInfo.role}
                </span>
              </div>

              <button
                onClick={onLogout}
                title="Sign out"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 5,
                  padding: '5px 10px',
                  background: 'var(--surface2)',
                  border: '1px solid var(--border)',
                  color: 'var(--muted)',
                  fontSize: 11,
                  fontWeight: 600,
                  borderRadius: 8,
                }}
              >
                <LogOut size={11} />
                Sign out
              </button>
            </div>
          ) : (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                color: 'var(--muted)',
                fontSize: 12,
              }}
            >
              <Activity size={13} />
              <span>AI-Native Multi-Cloud Scheduler</span>
            </div>
          )}
        </div>
      </header>

      <main
        style={{
          flex: 1,
          padding: '28px 32px',
          maxWidth: 1280,
          margin: '0 auto',
          width: '100%',
        }}
      >
        {children}
      </main>
    </div>
  )
}