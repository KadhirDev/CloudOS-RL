import { Moon, Sun } from 'lucide-react'
import { useTheme } from '../hooks/useTheme'

export default function ThemeToggle() {
  const { isDark, toggle } = useTheme()
  return (
    <button
      onClick={toggle}
      title={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
      style={{
        display: 'flex', alignItems: 'center', gap: 6,
        padding: '6px 12px',
        background: 'var(--surface2)',
        border: '1px solid var(--border)',
        color: 'var(--text2)',
        fontSize: 12, fontWeight: 600,
        borderRadius: 8,
      }}
    >
      {isDark ? <Sun size={13} /> : <Moon size={13} />}
      {isDark ? 'Light' : 'Dark'}
    </button>
  )
}