import axios from 'axios'

/**
 * Axios instance for CloudOS API
 */
const api = axios.create({
  baseURL: '/api/v1',
  timeout: 15000,
  headers: { 'Content-Type': 'application/json' },
})

/**
 * Attach Bearer token from localStorage if present
 */
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('cloudos_token')
  if (token) {
    config.headers = config.headers || {}
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

/**
 * Response + Error Interceptor
 * Safe for production and auth-aware
 */
api.interceptors.response.use(
  (response) => {
    try {
      console.debug(
        `[API] ${response?.config?.method?.toUpperCase()} ${response?.config?.url}`,
        response?.data
      )
    } catch (e) {
      console.debug('[API] Response log failed')
    }
    return response
  },
  (error) => {
    try {
      console.error(
        `[API] ERROR ${error?.response?.status || ''} ${error?.config?.url || ''}`,
        error?.response?.data || error?.message
      )
    } catch (e) {
      console.error('[API] Error log failed')
    }

    if (error?.response?.status === 401) {
      // Token expired or missing — clear and let AuthContext handle redirect
      localStorage.removeItem('cloudos_token')
      localStorage.removeItem('cloudos_user')
      window.dispatchEvent(new Event('cloudos:unauthorized'))
    }

    return Promise.reject(error)
  }
)

/**
 * Scheduling API
 * All return only response.data
 */
export const scheduleWorkload = (payload) =>
  api.post('/schedule', payload).then((r) => r.data)

export const getStatus = () =>
  api.get('/status').then((r) => r.data)

export const getDecisions = (limit = 20) =>
  api.get('/decisions', { params: { limit } }).then((r) => r.data)

export const getDecision = (id) =>
  api.get(`/decisions/${id}`).then((r) => r.data)

export const explainDecision = (id) =>
  api.post(`/decisions/${id}/explain`).then((r) => r.data)

/**
 * Auth API
 * Uses /auth base separately
 */
const authApi = axios.create({
  baseURL: '/auth',
  timeout: 10000,
})

export const login = (username, password) =>
  authApi.post('/login', { username, password }).then((r) => r.data)

export const getMe = () => {
  const token = localStorage.getItem('cloudos_token')
  return axios
    .get('/auth/me', {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    })
    .then((r) => r.data)
}