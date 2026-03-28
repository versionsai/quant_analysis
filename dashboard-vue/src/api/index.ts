const API_BASE = ''

class ApiClient {
  private async request<T>(url: string, options?: RequestInit): Promise<T> {
    const response = await fetch(`${API_BASE}${url}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      }
    })
    if (!response.ok) {
      throw new Error(`API Error: ${response.status}`)
    }
    return response.json()
  }

  get<T>(url: string): Promise<T> {
    return this.request<T>(url)
  }

  post<T>(url: string, data?: unknown): Promise<T> {
    return this.request<T>(url, {
      method: 'POST',
      body: data ? JSON.stringify(data) : undefined
    })
  }
}

export const api = new ApiClient()

// Overview
export const getOverview = () => api.get('/api/overview')
export const getRuntimeConfig = () => api.get('/api/runtime-config')
export const setRuntimeConfig = (data: any) => api.post('/api/runtime-config', data)

// Market
export const getMarket = () => api.get('/api/market')

// Holdings
export const getHoldings = () => api.get('/api/holdings')

// Signal Pool
export const getSignalPool = (limit = 50) => api.get(`/api/signal-pool?limit=${limit}`)
export const getSignalPoolAll = (limit = 100) => api.get(`/api/signal-pool-all?limit=${limit}`)

// Stock Pool
export const getStockPool = (limit = 50) => api.get(`/api/stock-pool?limit=${limit}`)

// Review
export const getSignalReview = (limit = 50) => api.get(`/api/signal-review?limit=${limit}`)
export const getTimingReview = (limit = 100) => api.get(`/api/timing-review?limit=${limit}`)
export const getReviewReport = () => api.get('/api/review-report')

// Timeline
export const getTimeline = (limit = 100) => api.get(`/api/timeline?limit=${limit}`)

// Trade Points
export const getTradePoints = (limit = 50) => api.get(`/api/trade-points?limit=${limit}`)

// Strategy Tuning
export const getStrategyTuning = () => api.get('/api/strategy-tuning')
export const getTimingExperiments = () => api.get('/api/timing-experiments')

// Dynamic Params
export const getDynamicParams = () => api.get('/api/dynamic-params')
export const setParam = (key: string, value: any) => api.post('/api/set-param', { key, value })

// Override
export const getOverrideHistory = (limit = 20) => api.get(`/api/override-history?limit=${limit}`)
export const addOverride = (data: any) => api.post('/api/add-override', data)

// Daily Optimization
export const getDailyOptimization = () => api.get('/api/daily-optimization')

// ETF Pool Status
export const getEtfPoolStatus = () => api.get('/api/etf-pool-status')

// TACO
export const getTacoHotTopics = () => api.get('/api/taco-hot-topics')

// Action
export const runAction = (action: string) => api.post('/api/action', { action })

// Health
export const getHealth = () => api.get('/api/health')
