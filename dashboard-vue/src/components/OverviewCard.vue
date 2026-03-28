<template>
  <n-card title="总览" class="overview-card">
    <n-grid :cols="4" :x-gap="16" :y-gap="8">
      <n-gi>
        <div class="stat-item">
          <div class="stat-label">持仓</div>
          <div class="stat-value">{{ summary?.holdings_count || 0 }}</div>
        </div>
      </n-gi>
      <n-gi>
        <div class="stat-item">
          <div class="stat-label">持仓盈亏</div>
          <div class="stat-value" :class="pnlClass">
            {{ formatPnl(summary?.holdings_pnl_pct) }}
          </div>
        </div>
      </n-gi>
      <n-gi>
        <div class="stat-item">
          <div class="stat-label">信号池</div>
          <div class="stat-value">{{ summary?.signal_pool_count || 0 }}</div>
        </div>
      </n-gi>
      <n-gi>
        <div class="stat-item">
          <div class="stat-label">股票池</div>
          <div class="stat-value">{{ summary?.stock_pool_count || 0 }}</div>
        </div>
      </n-gi>
    </n-grid>
    <template #footer>
      <n-space justify="space-between">
        <span class="muted">行情缓存: {{ summary?.refresh_market_cache_at || '--' }}</span>
        <n-button size="small" @click="$emit('refresh')">刷新</n-button>
      </n-space>
    </template>
  </n-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { NCard, NGrid, NGi, NSpace, NButton } from 'naive-ui'

const props = defineProps<{
  data: any
}>()

defineEmits(['refresh'])

const summary = computed(() => props.data?.summary || {})

const pnlClass = computed(() => {
  const pnl = summary.value.holdings_pnl_pct
  if (pnl > 0) return 'positive'
  if (pnl < 0) return 'negative'
  return ''
})

const formatPnl = (val: number) => {
  if (val === undefined || val === null) return '--'
  return `${val > 0 ? '+' : ''}${val.toFixed(2)}%`
}
</script>

<style scoped>
.overview-card {
  background: rgba(255,255,255,0.9);
}

.stat-item {
  text-align: center;
}

.stat-label {
  font-size: 12px;
  color: #6f7d95;
  margin-bottom: 4px;
}

.stat-value {
  font-size: 24px;
  font-weight: 600;
  color: #162033;
}

.stat-value.positive {
  color: #2fbf71;
}

.stat-value.negative {
  color: #ff6b6b;
}

.muted {
  color: #6f7d95;
  font-size: 12px;
}
</style>
