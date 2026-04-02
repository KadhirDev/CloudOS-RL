/**
 * Layout
 * ======
 * Safe merged version:
 * - Preserves existing header branding and user/logout flow
 * - Adds optional sidebar support without introducing a new required file
 * - Backward compatible: existing usages continue to work
 * - Default showSidebar=false to avoid changing current pages unexpectedly
 */

import { useMemo, useState } from 'react'
import {
  Activity,
  Zap,
  LogOut,
  User,
  Bell,
  LayoutDashboard,
  BarChart3,
  Cpu,
  ShieldCheck,
  Cloud,
} from 'lucide-react'

function InlineSidebar({ activeSection, onNavigate, userInfo }) {
  const items = useMemo(() => {
    const base = [
      { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
      { id: 'metrics', label: 'Metrics', icon: BarChart3 },
      { id: 'decisions', label: 'Decisions', icon: Cpu },
    ]

    if (userInfo?.role === 'admin') {
      base.push({ id: 'admin', label: 'Admin', icon: ShieldCheck })
    }

    if (userInfo?.role === 'executive') {
      base.push({ id: 'snapshot', label: 'Snapshot', icon: Cloud })
    }

    return base
  }, [userInfo])

  return (
    <aside
      style={{
        width: 220,
        borderRight: '1px solid var(--border)',
        background: 'var(--surface)',
        padding: '18px 14px',
        flexShrink: 0,
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          marginBottom: 22,
          padding: '0 6px',
        }}
      >
        <div
          style={{
            width: 34,
            height: 34,
            borderRadius: 10,
            background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 6px 18px rgba(0,0,0,0.12)',
          }}
        >
          <Zap size={16} color="#fff" />
        </div>

        <div>
          <div style={{ fontWeight: 800, fontSize: 14, lineHeight: 1.1 }}>CloudOS</div>
          <div
            style={{
              fontSize: 10,
              color: 'var(--muted)',
              textTransform: 'uppercase',
              letterSpacing: '0.08em',
              fontWeight: 700,
              marginTop: 2,
            }}
          >
            RL Platform
          </div>
        </div>
      </div>

      <div
        style={{
          fontSize: 11,
          color: 'var(--muted)',
          textTransform: 'uppercase',
          letterSpacing: '0.08em',
          fontWeight: 700,
          margin: '0 6px 10px',
        }}
      >
        Navigation
      </div>

      <nav style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {items.map(({ id, label, icon: Icon }) => {
          const active = activeSection === id

          return (
            <button
              key={id}
              onClick={() => onNavigate(id)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                width: '100%',
                padding: '10px 12px',
                borderRadius: 10,
                border: `1px solid ${active ? 'var(--accent2)' : 'var(--border)'}`,
                background: active ? 'var(--surface2)' : 'transparent',
                color: active ? 'var(--text)' : 'var(--muted)',
                fontSize: 13,
                fontWeight: active ? 700 : 600,
                cursor: 'pointer',
                textAlign: 'left',
              }}
            >
              <Icon size={14} />
              <span>{label}</span>
            </button>
          )
        })}
      </nav>

      <div
        style={{
          marginTop: 20,
          padding: '12px 10px',
          borderRadius: 10,
          background: 'var(--surface2)',
          border: '1px solid var(--border)',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            fontSize: 12,
            color: 'var(--muted)',
          }}
        >
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: '50%',
              background: 'var(--green)',
              boxShadow: '0 0 8px var(--green)',
              display: 'inline-block',
            }}
          />
          <span>AI Scheduler Live</span>
        </div>
      </div>
    </aside>
  )
}

export default function Layout({
  children,
  userInfo,
  onLogout,
  headerExtra,
  showSidebar = false,
}) {
  const [activeSection, setActiveSection] = useState('dashboard')

  const ROLE_COLORS = {
    viewer: 'var(--muted)',
    user: 'var(--accent)',
    engineer: 'var(--green)',
    admin: 'var(--red)',
    executive: 'var(--accent2)',
  }

  const handleNavigate = (sectionId) => {
    setActiveSection(sectionId)

    if (typeof document !== 'undefined') {
      const el = document.getElementById(`section-${sectionId}`)
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' })
      }
    }
  }

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <header
        style={{
          background: 'var(--surface)',
          borderBottom: '1px solid var(--border)',
          padding: '0 24px 0 32px',
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
          {!showSidebar ? (
            <>
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
            </>
          ) : (
            <div
              style={{
                fontSize: 12,
                color: 'var(--muted)',
                display: 'flex',
                alignItems: 'center',
                gap: 6,
              }}
            >
              <span
                style={{
                  width: 7,
                  height: 7,
                  borderRadius: '50%',
                  background: 'var(--green)',
                  boxShadow: '0 0 6px var(--green)',
                  display: 'inline-block',
                }}
              />
              <span>AI-Native Multi-Cloud Scheduler</span>
            </div>
          )}
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          {headerExtra}

          <button
            type="button"
            title="Notifications"
            style={{
              background: 'var(--surface2)',
              border: '1px solid var(--border)',
              color: 'var(--muted)',
              padding: '6px 8px',
              borderRadius: 8,
              display: 'flex',
              alignItems: 'center',
              cursor: 'pointer',
            }}
          >
            <Bell size={13} />
          </button>

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
                type="button"
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
                  cursor: 'pointer',
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

      <div style={{ display: 'flex', flex: 1 }}>
        {showSidebar && userInfo && (
          <InlineSidebar
            activeSection={activeSection}
            onNavigate={handleNavigate}
            userInfo={userInfo}
          />
        )}

        <main
          style={{
            flex: 1,
            padding: '28px 32px',
            maxWidth: showSidebar ? '100%' : 1280,
            margin: showSidebar ? 0 : '0 auto',
            width: '100%',
            overflowX: 'hidden',
          }}
        >
          {children}
        </main>
      </div>
    </div>
  )
}