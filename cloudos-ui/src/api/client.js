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
 * Response + Error Interceptor (Debugging)
 * Safe for production (no breaking behavior)
 */
api.interceptors.response.use(
  (response) => {
    try {
      console.debug(
        `[API] ${response?.config?.method?.toUpperCase()} ${response?.config?.url}`,
        response?.data
      )
    } catch (e) {
      // Prevent logging crash
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

    return Promise.reject(error)
  }
)

/**
 * API Calls
 * NOTE: All return only response.data (no breaking change)
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