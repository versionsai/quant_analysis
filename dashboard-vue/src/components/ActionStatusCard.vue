<template>
  <n-card title="操作执行状态">
    <n-space vertical>
      <n-space v-for="(item, key) in data" :key="key" justify="space-between">
        <span>{{ formatKey(key) }}</span>
        <n-tag :type="getStatusType(item?.status)">
          {{ item?.status || 'unknown' }}
        </n-tag>
      </n-space>
      <div v-if="!Object.keys(data || {}).length" class="muted">
        暂无执行状态
      </div>
    </n-space>
  </n-card>
</template>

<script setup lang="ts">
import { NCard, NSpace, NTag } from 'naive-ui'

defineProps<{
  data: Record<string, any>
}>()

const formatKey = (key: string) => {
  const map: Record<string, string> = {
    refresh_market_cache: '刷新行情缓存',
    refresh_pool: '刷新股票池',
    refresh_signal_pool: '刷新信号池',
    refresh_timing_experiments: '刷新择时试验',
  }
  return map[key] || key
}

const getStatusType = (status: string) => {
  if (status === 'success') return 'success'
  if (status === 'failed') return 'error'
  return 'default'
}
</script>

<style scoped>
.muted {
  color: #6f7d95;
  text-align: center;
  padding: 20px;
}
</style>
