<template>
  <n-card title="综合复盘报告">
    <n-spin :show="loading">
      <pre class="report-text">{{ reportText }}</pre>
    </n-spin>
    <template #footer>
      <n-button size="small" @click="loadReport">刷新</n-button>
    </template>
  </n-card>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { NCard, NSpin, NButton } from 'naive-ui'
import { getReviewReport } from '../api'

const loading = ref(false)
const data = ref<any>({})

const reportText = computed(() => data.value?.report_text || '暂无报告')

const loadReport = async () => {
  loading.value = true
  try {
    data.value = await getReviewReport()
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadReport()
})
</script>

<style scoped>
.report-text {
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 13px;
  line-height: 1.6;
  max-height: 400px;
  overflow-y: auto;
}
</style>
