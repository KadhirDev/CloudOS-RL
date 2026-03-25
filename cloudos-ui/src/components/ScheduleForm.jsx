import { useState } from 'react'
import { Send, Loader } from 'lucide-react'
import { scheduleWorkload } from '../api/client'

const DEFAULTS = {
  workload_type: 'training',
  cpu_request_vcpu: 4,
  memory_request_gb: 8,
  gpu_count: 0,
  storage_gb: 100,
  expected_duration_hours: 2,
  priority: 2,
  sla_latency_ms: 200,
  is_spot_tolerant: false,
}

export default function ScheduleForm({ onResult }) {
  const [form, setForm] = useState(DEFAULTS)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const set = (k, v) => setForm((f) => ({ ...f, [k]: v }))

  const submit = async () => {
    setLoading(true)
    setError(null)

    try {
      const result = await scheduleWorkload({
        ...form,
        cpu_request_vcpu: Number(form.cpu_request_vcpu),
        memory_request_gb: Number(form.memory_request_gb),
        gpu_count: Number(form.gpu_count),
        storage_gb: Number(form.storage_gb),
        expected_duration_hours: Number(form.expected_duration_hours),
        priority: Number(form.priority),
        sla_latency_ms: Number(form.sla_latency_ms),
      })

      // Debug log so browser console shows raw API response
      console.log('[CloudOS] Schedule response:', result)

      if (!result || !result.decision_id) {
        setError('API returned an unexpected response format. Check browser console.')
        return
      }

      if (typeof onResult === 'function') {
        onResult(result)
      }
    } catch (e) {
      // Show the actual server error message
      const detail =
        e?.response?.data?.detail ||
        e?.response?.data?.message ||
        e?.message ||
        'Unknown error'

      const status = e?.response?.status || ''

      console.error('[CloudOS] Schedule error:', e?.response?.data || e)
      setError(`${status ? `[${status}] ` : ''}${detail}`)
    } finally {
      setLoading(false)
    }
  }

  const row = { display: 'flex', gap: 14, marginBottom: 14 }
  const col = { flex: 1 }

  return (
    <div className="card">
      <div style={{ marginBottom: 18, display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ fontWeight: 600, fontSize: 15 }}>Submit Workload</span>
        <span className="badge badge-blue">PPO Scheduler</span>
      </div>

      <div style={row}>
        <div style={col}>
          <label>Workload Type</label>
          <select
            value={form.workload_type}
            onChange={(e) => set('workload_type', e.target.value)}
          >
            <option value="training">Training</option>
            <option value="inference">Inference</option>
            <option value="batch">Batch</option>
            <option value="streaming">Streaming</option>
          </select>
        </div>
        <div style={col}>
          <label>Priority (1–4)</label>
          <select
            value={form.priority}
            onChange={(e) => set('priority', e.target.value)}
          >
            <option value={1}>1 — Low</option>
            <option value={2}>2 — Normal</option>
            <option value={3}>3 — High</option>
            <option value={4}>4 — Critical</option>
          </select>
        </div>
      </div>

      <div style={row}>
        <div style={col}>
          <label>CPU (vCPU)</label>
          <input
            type="number"
            min={0.25}
            step={0.25}
            value={form.cpu_request_vcpu}
            onChange={(e) => set('cpu_request_vcpu', e.target.value)}
          />
        </div>
        <div style={col}>
          <label>Memory (GB)</label>
          <input
            type="number"
            min={0.5}
            step={0.5}
            value={form.memory_request_gb}
            onChange={(e) => set('memory_request_gb', e.target.value)}
          />
        </div>
        <div style={col}>
          <label>GPU Count</label>
          <input
            type="number"
            min={0}
            max={16}
            value={form.gpu_count}
            onChange={(e) => set('gpu_count', e.target.value)}
          />
        </div>
      </div>

      <div style={row}>
        <div style={col}>
          <label>Storage (GB)</label>
          <input
            type="number"
            min={1}
            value={form.storage_gb}
            onChange={(e) => set('storage_gb', e.target.value)}
          />
        </div>
        <div style={col}>
          <label>Duration (hours)</label>
          <input
            type="number"
            min={0.1}
            step={0.1}
            value={form.expected_duration_hours}
            onChange={(e) => set('expected_duration_hours', e.target.value)}
          />
        </div>
        <div style={col}>
          <label>SLA Latency (ms)</label>
          <input
            type="number"
            min={10}
            value={form.sla_latency_ms}
            onChange={(e) => set('sla_latency_ms', e.target.value)}
          />
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 18 }}>
        <button
          onClick={() => set('is_spot_tolerant', !form.is_spot_tolerant)}
          style={{
            padding: '7px 14px',
            background: form.is_spot_tolerant ? '#10b98120' : 'var(--surface2)',
            border: `1px solid ${
              form.is_spot_tolerant ? 'var(--green)' : 'var(--border)'
            }`,
            color: form.is_spot_tolerant ? 'var(--green)' : 'var(--muted)',
            fontWeight: 600,
          }}
        >
          {form.is_spot_tolerant ? '✓ Spot Tolerant' : 'Spot Tolerant'}
        </button>
        <span style={{ color: 'var(--muted)', fontSize: 12 }}>
          {form.is_spot_tolerant
            ? 'Eligible for spot pricing (up to 70% savings)'
            : 'On-demand only'}
        </span>
      </div>

      {error && (
        <div
          style={{
            background: '#ef444415',
            border: '1px solid var(--red)',
            borderRadius: 8,
            padding: '10px 14px',
            color: 'var(--red)',
            fontSize: 12,
            marginBottom: 14,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
          }}
        >
          {error}
        </div>
      )}

      <button
        onClick={submit}
        disabled={loading}
        style={{
          width: '100%',
          padding: '11px 0',
          background: 'linear-gradient(135deg, var(--accent), var(--accent2))',
          color: '#fff',
          fontWeight: 600,
          fontSize: 14,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: 8,
        }}
      >
        {loading ? (
          <>
            <Loader size={15} style={{ animation: 'spin 1s linear infinite' }} />
            Scheduling…
          </>
        ) : (
          <>
            <Send size={15} />
            Schedule Workload
          </>
        )}
      </button>

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}