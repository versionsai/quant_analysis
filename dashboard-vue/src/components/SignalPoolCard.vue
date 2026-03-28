<template>
  <n-card title="信号池">
    <n-data-table
      :columns="columns"
      :data="displayRows"
      :pagination="false"
      :bordered="false"
      :max-height="300"
    />
    <template #footer>
      <span class="muted">
        活跃 {{ counts?.active || 0 }} / 持有中 {{ counts?.holding || 0 }} / 已失效 {{ counts?.inactive || 0 }}
      </span>
    </template>
  </n-card>
</template>

<script setup lang="ts">
import { computed, h } from 'vue'
import { NCard, NDataTable, NSpace } from 'naive-ui'

const props = defineProps<{
  data: any
}>()

const counts = computed(() => props.data?.counts || {})
const displayRows = computed(() => props.data?.display_rows || [])

const columns = [
  { title: '代码', key: 'code', width: 80 },
  { title: '名称', key: 'name', width: 80 },
  { title: '信号', key: 'signal_type', width: 60 },
  { 
    title: '日期', 
    key: 'date',
    render: (row: any) => row.date?.slice(0, 10) || '--'
  },
]
</script>

<style scoped>
.muted {
  color: #6f7d95;
  font-size: 12px;
}
</style>
