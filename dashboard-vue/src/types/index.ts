export interface Overview {
  summary: {
    holdings_count: number
    holdings_pnl_pct: number
    holdings_pnl_value: number
    signal_pool_count: number
    signal_pool_holding_count: number
    signal_pool_inactive_count: number
    stock_pool_count: number
    refresh_market_cache_at: string
    refresh_pool_at: string
    refresh_signal_pool_at: string
    [key: string]: any
  }
  [key: string]: any
}

export interface MarketData {
  indices: any[]
  etfs: any[]
  holdings: any[]
  [key: string]: any
}

export interface Holding {
  code: string
  name: string
  volume: number
  avg_cost: number
  current_price: number
  pnl_pct: number
  pnl_value: number
  [key: string]: any
}

export interface Signal {
  code: string
  name: string
  date: string
  signal_type: string
  weight: number
  [key: string]: any
}

export interface StockPoolItem {
  code: string
  name: string
  pool_type: string
  updated_at: string
  [key: string]: any
}

export interface TimelineItem {
  id: number
  timestamp: string
  action: string
  result: string
  [key: string]: any
}

export interface ReviewRecord {
  date: string
  code: string
  name: string
  signal_type: string
  result: string
  pnl_pct: number
  [key: string]: any
}

export interface TACOItem {
  name: string
  reason: string
  keywords: string[]
  score: number
  date: string
  source: string
  topic_group: string
  [key: string]: any
}

export interface DynamicParam {
  key: string
  value: any
  source: string
  [key: string]: any
}

export interface ActionState {
  [key: string]: {
    status: string
    message: string
    timestamp: string
  }
}
