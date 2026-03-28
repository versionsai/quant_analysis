<template>
  <n-card title="实时行情">
    <n-spin :show="loading">
      <n-tabs type="segment">
        <n-tab-pane name="indices" tab="指数">
          <n-space vertical>
            <n-tag v-for="item in data?.indices || []" :key="item.code" :type="getType(item.change_rate)">
              {{ item.name }} {{ item.price }} ({{ formatChange(item.change_rate) }})
            </n-tag>
          </n-space>
        </n-tab-pane>
        <n-tab-pane name="etfs" tab="ETF">
          <n-space vertical>
            <n-tag v-for="item in data?.etfs || []" :key="item.code" :type="getType(item.change_rate)">
              {{ item.name }} {{ item.price }} ({{ formatChange(item.change_rate) }})
            </n-tag>
          </n-space>
        </n-tab-pane>
      </n-tabs>
    </n-spin>
  </n-card>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { NCard, NTabs, NTabPane, NSpace, NTag, NSpin } from 'naive-ui'
import { getMarket } from '../api'

const props = defineProps<{
  data?: any
}>()

const loading = ref(false)

const getType = (rate: number) => {
  if (rate > 0) return 'success'
  if (rate < 0) return 'error'
  return 'default'
}

const formatChange = (val: number) => {
  if (val === undefined || val === null) return '--'
  return `${val > 0 ? '+' : ''}${val.toFixed(2)}%`
}
</script>
