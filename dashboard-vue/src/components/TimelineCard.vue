<template>
  <n-card title="交易事件时间线">
    <n-timeline>
      <n-timeline-item 
        v-for="item in data" 
        :key="item.id"
        :title="item.action"
        :time="item.timestamp"
        :type="getType(item.action)"
      >
        {{ item.result }}
      </n-timeline-item>
    </n-timeline>
    <template #footer>
      <span class="muted">共 {{ (data || []).length }} 条记录</span>
    </template>
  </n-card>
</template>

<script setup lang="ts">
import { NCard, NTimeline, NTimelineItem } from 'naive-ui'

defineProps<{
  data: any[]
}>()

const getType = (action: string) => {
  if (action?.includes('买入') || action?.includes('信号')) return 'success'
  if (action?.includes('卖出')) return 'warning'
  return 'info'
}
</script>

<style scoped>
.muted {
  color: #6f7d95;
  font-size: 12px;
}
</style>
