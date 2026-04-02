/**
 * Sidebar — role-aware navigation
 * Shown in engineer, admin, and executive views.
 * Navigation items scroll to named sections or activate view state.
 */

import { useState } from 'react'
import {
  LayoutDashboard, Send, ListChecks, Zap,
  BarChart2, Activity, DollarSign, Leaf,
  Settings, ChevronLeft, ChevronRight,
} from 'lucide-react'
import { useAuth } from '../auth/AuthContext'

const NAV_ITEMS = [
  { id: 'dashboard',    label: 'Dashboard',       icon: LayoutDashboard, roles: ['engineer','admin','executive','viewer','user'] },
  { id: 'schedule',     label: 'Schedule',        icon: Send,            roles: ['engineer','admin','user'] },
  { id: 'decisions',    label: 'Decisions',       icon: ListChecks,      roles: ['engineer','admin','executive','viewer','user'] },
  { id: 'explainability',label:'Explainability',  icon: Zap,             roles: ['engineer','admin'] },
  { id: 'metrics',      label: 'Metrics',         icon: BarChart2,       roles: ['engineer','admin','executive'] },
  { id: 'kafka',        label: 'Kafka Events',    icon: Activity,        roles: ['engineer','admin'] },
  { id: 'cost',         label: 'Cost Insights',   icon: DollarSign,      roles: ['engineer','admin','executive'] },
  { id: 'carbon',       label: 'Carbon Insights', icon: Leaf,            roles: ['engineer','admin','executive'] },
]

export default function Sidebar({ activeSection, onNavigate }) {
  const { user }        = useAuth()
  const [collapsed, setCollapsed] = useState(false)
  const role = user?.role || 'viewer'

  const visible = NAV_ITEMS.filter(item => item.roles.includes(role))

  return (
    <aside style={{
      width:        collapsed ? 56 : 220,
      minHeight:    '100vh',
      background:   'var(--surface)',
      borderRight:  '1px solid var(--border)',
      display:      'flex',
      flexDirection:'column',
      position:     'sticky',
      top:          0,
      transition:   'width 0.2s ease',
      flexShrink:   0,
      zIndex:       50,
    }}>
      {/* Logo area */}
      <div style={{
        height:     56, display: 'flex',
        alignItems: 'center',
        padding:    collapsed ? '0 12px' : '0 16px',
        borderBottom: '1px solid var(--border)',
        justifyContent: collapsed ? 'center' : 'space-between',
      }}>
        {!collapsed && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div style={{
              width: 28, height: 28, borderRadius: 7,
              background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <Zap size={14} color="#fff" />
            </div>
            <div>
              <div style={{ fontWeight: 800, fontSize: 13, lineHeight: 1 }}>CloudOS</div>
              <div style={{ fontSize: 9, color: 'var(--accent)', fontWeight: 700, letterSpacing: '0.08em' }}>RL SCHEDULER</div>
            </div>
          </div>
        )}
        <button
          onClick={() => setCollapsed(c => !c)}
          title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          style={{
            background: 'var(--surface2)', border: '1px solid var(--border)',
            color: 'var(--muted)', padding: 4, borderRadius: 6,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}
        >
          {collapsed ? <ChevronRight size={12} /> : <ChevronLeft size={12} />}
        </button>
      </div>

      {/* Navigation items */}
      <nav style={{ flex: 1, padding: '12px 8px', display: 'flex', flexDirection: 'column', gap: 2 }}>
        {visible.map(item => {
          const isActive = activeSection === item.id
          return (
            <button
              key={item.id}
              onClick={() => onNavigate?.(item.id)}
              title={collapsed ? item.label : undefined}
              style={{
                display:     'flex',
                alignItems:  'center',
                gap:         10,
                padding:     collapsed ? '9px 0' : '9px 12px',
                justifyContent: collapsed ? 'center' : 'flex-start',
                borderRadius: 8,
                fontWeight:   isActive ? 700 : 500,
                fontSize:     13,
                color:        isActive ? 'var(--accent)' : 'var(--text2)',
                background:   isActive ? 'rgba(59,130,246,0.1)' : 'transparent',
                border:       `1px solid ${isActive ? 'rgba(59,130,246,0.2)' : 'transparent'}`,
                transition:   'all 0.12s',
                width:        '100%',
                whiteSpace:   'nowrap',
                overflow:     'hidden',
              }}
              onMouseEnter={e => {
                if (!isActive) {
                  e.currentTarget.style.background = 'var(--surface2)'
                  e.currentTarget.style.color = 'var(--text)'
                }
              }}
              onMouseLeave={e => {
                if (!isActive) {
                  e.currentTarget.style.background = 'transparent'
                  e.currentTarget.style.color = 'var(--text2)'
                }
              }}
            >
              <item.icon size={15} style={{ flexShrink: 0 }} />
              {!collapsed && <span>{item.label}</span>}
            </button>
          )
        })}
      </nav>

      {/* Bottom: role badge */}
      {!collapsed && (
        <div style={{
          padding: '12px 16px',
          borderTop: '1px solid var(--border)',
          fontSize: 11,
        }}>
          <div style={{ color: 'var(--muted)', marginBottom: 3 }}>Signed in as</div>
          <div style={{ fontWeight: 700, color: 'var(--text)', fontSize: 12 }}>{user?.username}</div>
          <div style={{
            display: 'inline-block', marginTop: 4,
            background: 'rgba(99,102,241,0.12)', color: 'var(--accent2)',
            border: '1px solid rgba(99,102,241,0.25)',
            borderRadius: 12, padding: '1px 8px',
            fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em',
          }}>
            {user?.role}
          </div>
        </div>
      )}
    </aside>
  )
}