<template>
  <n-card title="当前持仓">
    <n-data-table
      :columns="columns"
      :data="data || []"
      :pagination="false"
      :bordered="false"
    />
    <template #footer>
      <span class="muted">共 {{ (data || []).length }} 只持仓</span>
    </template>
  </n-card>
</template>

<script setup lang="ts">
import { h } from 'vue'
import { NCard, NDataTable, NSpace } from 'naive-ui'

defineProps<{
  data: any[]
}>()

const columns = [
  { title: '代码', key: 'code', width: 80 },
  { title: '名称', key: 'name', width: 100 },
  { 
    title: '现价', 
    key: 'current_price',
    render: (row: any) => row.current_price?.toFixed(3) || '--'
  },
  { 
    title: '盈亏', 
    key: 'pnl_pct',
    render: (row: any) => {
      const val = row.pnl_pct
      if (val === undefined || val === null) return '--'
      return h('span', { style: { color: val >= 0 ? '#2fbf71' : '#ff6b6b' } }, 
        `${val > 0 ? '+' : ''}${val.toFixed(2)}%`
      )
    }
  },
]
</script>

<style scoped>
.muted {
  color: #6f7d95;
  font-size: 12px;
}
</style>
