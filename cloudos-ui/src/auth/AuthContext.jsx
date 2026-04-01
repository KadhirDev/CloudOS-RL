import { createContext, useContext, useState, useEffect, useCallback } from 'react'
import { login as apiLogin } from '../api/client'

const AuthContext = createContext(null)

export const ROLE_LABELS = {
  viewer:    'Viewer',
  user:      'User',
  engineer:  'Engineer',
  admin:     'Admin',
  executive: 'Executive',
}

// What each role can do in the UI
export const PERMISSIONS = {
  viewer:    { canSchedule: false, canViewEngineering: false, canViewExecutive: false, canAdmin: false },
  user:      { canSchedule: true,  canViewEngineering: false, canViewExecutive: false, canAdmin: false },
  engineer:  { canSchedule: true,  canViewEngineering: true,  canViewExecutive: false, canAdmin: false },
  admin:     { canSchedule: true,  canViewEngineering: true,  canViewExecutive: true,  canAdmin: true  },
  executive: { canSchedule: false, canViewEngineering: false, canViewExecutive: true,  canAdmin: false },
}

export function AuthProvider({ children }) {
  const [user,  setUser]  = useState(null)   // { username, role, token }
  const [ready, setReady] = useState(false)

  // Restore session from localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem('cloudos_user')
      if (stored) {
        const parsed = JSON.parse(stored)
        setUser(parsed)
      }
    } catch {}
    setReady(true)
  }, [])

  // Listen for 401 events from API interceptor
  useEffect(() => {
    const handler = () => { setUser(null) }
    window.addEventListener('cloudos:unauthorized', handler)
    return () => window.removeEventListener('cloudos:unauthorized', handler)
  }, [])

  const login = useCallback(async (username, password) => {
    const data = await apiLogin(username, password)
    const userObj = { username: data.username, role: data.role, token: data.access_token }
    localStorage.setItem('cloudos_token', data.access_token)
    localStorage.setItem('cloudos_user',  JSON.stringify(userObj))
    setUser(userObj)
    return userObj
  }, [])

  const logout = useCallback(() => {
    localStorage.removeItem('cloudos_token')
    localStorage.removeItem('cloudos_user')
    setUser(null)
  }, [])

  const perms = user ? (PERMISSIONS[user.role] || PERMISSIONS.viewer) : PERMISSIONS.viewer

  return (
    <AuthContext.Provider value={{ user, perms, login, logout, ready }}>
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = () => useContext(AuthContext)