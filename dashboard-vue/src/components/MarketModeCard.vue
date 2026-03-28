<template>
  <n-card title="市场模式">
    <n-space vertical>
      <n-statistic label="当前模式">
        <template #default>
          <span class="mode-label">{{ summary?.market_regime_label || '自动' }}</span>
        </template>
      </n-statistic>
      <n-radio-group v-model:value="mode" @update:value="handleModeChange">
        <n-space>
          <n-radio value="auto">自动</n-radio>
          <n-radio value="bull">牛市</n-radio>
          <n-radio value="bear">熊市</n-radio>
          <n-radio value="震荡">震荡</n-radio>
        </n-space>
      </n-radio-group>
    </n-space>
  </n-card>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { NCard, NSpace, NStatistic, NRadioGroup, NRadio } from 'naive-ui'

const props = defineProps<{
  data: any
}>()

const summary = computed(() => props.data?.summary || {})
const mode = ref(summary.value.market_regime_mode || 'auto')

const handleModeChange = (value: string) => {
  console.log('Mode changed:', value)
}
</script>

<style scoped>
.mode-label {
  font-size: 24px;
  font-weight: 600;
  color: #2563eb;
}
</style>
