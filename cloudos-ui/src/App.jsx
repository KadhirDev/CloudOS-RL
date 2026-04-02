import { useEffect } from 'react'
import { AuthProvider, useAuth } from './auth/AuthContext'
import LoginPage from './auth/LoginPage'
import Layout from './components/Layout'
import ThemeToggle from './components/ThemeToggle'
import BasicView from './views/BasicView'
import EngineerView from './views/EngineerView'
import ExecutiveView from './views/ExecutiveView'
import ToastContainer from './components/Toast'
import { useTheme } from './hooks/useTheme'

function ThemeInit() {
  const { theme } = useTheme()

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  return null
}

function resolveViewByRole(role) {
  if (role === 'executive') return ExecutiveView
  if (role === 'engineer' || role === 'admin' || role === 'user') return EngineerView
  return BasicView
}

function shouldShowSidebar(role) {
  // Keep viewer/simple roles on the lightweight layout.
  return role !== 'viewer'
}

function AppInner() {
  const { user, logout, ready } = useAuth()

  if (!ready) return null
  if (!user) return <LoginPage />

  const role = user?.role || 'viewer'
  const ViewComponent = resolveViewByRole(role)
  const showSidebar = shouldShowSidebar(role)

  return (
    <Layout
      userInfo={user}
      onLogout={logout}
      headerExtra={<ThemeToggle />}
      showSidebar={showSidebar}
    >
      <ViewComponent />
    </Layout>
  )
}

export default function App() {
  return (
    <AuthProvider>
      <ThemeInit />
      <AppInner />
      <ToastContainer />
    </AuthProvider>
  )
}