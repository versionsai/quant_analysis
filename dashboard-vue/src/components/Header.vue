<template>
  <n-card class="header-card">
    <n-space justify="space-between" align="center">
      <div>
        <h1 class="title">量化信号看板</h1>
        <p class="subtitle">实时监控 · AI分析 · 自动推送</p>
      </div>
      <n-space>
        <n-button @click="$emit('refresh')" :loading="loading">
          刷新
        </n-button>
        <n-dropdown :options="actionOptions" @select="handleAction">
          <n-button>执行操作</n-button>
        </n-dropdown>
      </n-space>
    </n-space>
    <div class="refresh-info">
      自动刷新：30秒
    </div>
  </n-card>
</template>

<script setup lang="ts">
import { h } from 'vue'
import { NButton, NSpace, NDropdown, NCard } from 'naive-ui'

defineProps<{
  loading: boolean
}>()

const emit = defineEmits(['refresh', 'action'])

const actionOptions = [
  { label: '刷新行情缓存', key: 'refresh_market_cache' },
  { label: '刷新股票池', key: 'refresh_pool' },
  { label: '刷新信号池', key: 'refresh_signal_pool' },
  { label: '刷新择时试验', key: 'refresh_timing_experiments' },
]

const handleAction = (key: string) => {
  emit('action', key)
}
</script>

<style scoped>
.header-card {
  margin-bottom: 16px;
  background: linear-gradient(135deg, rgba(255,255,255,0.92), rgba(241,246,255,0.86));
}

.title {
  margin: 0 0 8px;
  font-size: 32px;
  color: #162033;
}

.subtitle {
  margin: 0;
  color: #6f7d95;
}

.refresh-info {
  margin-top: 12px;
  color: #6f7d95;
  font-size: 12px;
}
</style>
