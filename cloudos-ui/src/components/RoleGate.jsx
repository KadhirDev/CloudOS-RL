import { useAuth } from '../auth/AuthContext'

/**
 * RoleGate — renders children only if user has one of the allowed roles.
 * Falls back to `fallback` prop or null.
 *
 * Usage:
 *   <RoleGate allow={['engineer', 'admin']}>
 *     <ScheduleForm />
 *   </RoleGate>
 */
export default function RoleGate({ allow = [], fallback = null, children }) {
  const { user } = useAuth()
  if (!user) return fallback
  if (!allow.includes(user.role)) return fallback
  return children
}