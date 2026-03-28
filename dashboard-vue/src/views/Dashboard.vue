<template>
  <div class="page">
    <Header :loading="loading" @refresh="refreshAll" @action="runAction" />
    
    <n-spin :show="loading">
      <n-grid :cols="2" :x-gap="16" :y-gap="16" responsive="screen" item-responsive>
        <n-gi span="2 m:1">
          <OverviewCard :data="overview" @refresh="refreshOverview" />
        </n-gi>
        <n-gi span="2 m:1">
          <MarketCards :data="market" />
        </n-gi>
        <n-gi span="2 m:1">
          <HoldingsCard :data="holdings" />
        </n-gi>
        <n-gi span="2 m:1">
          <ActionStatusCard :data="actionStatus" />
        </n-gi>
        <n-gi span="2 m:1">
          <MarketModeCard :data="overview" />
        </n-gi>
        <n-gi span="2 m:1">
          <EtfPoolStatusCard :data="etfPoolStatus" />
        </n-gi>
        <n-gi span="2">
          <TacoHotTopicsCard />
        </n-gi>
        <n-gi span="2 m:1">
          <SignalPoolCard :data="signalPool" />
        </n-gi>
        <n-gi span="2 m:1">
          <StockPoolCard :data="stockPool" />
        </n-gi>
        <n-gi span="2">
          <SignalReviewCard :data="signalReview" />
        </n-gi>
        <n-gi span="2">
          <TimingReviewCard :data="timingReview" />
        </n-gi>
        <n-gi span="2">
          <TimelineCard :data="timeline" />
        </n-gi>
        <n-gi span="2">
          <ReviewReportCard :data="reviewReport" />
        </n-gi>
        <n-gi span="2 m:1">
          <DynamicParamsCard @refresh="refreshDynamicParams" />
        </n-gi>
        <n-gi span="2 m:1">
          <OverrideHistoryCard />
        </n-gi>
      </n-grid>
    </n-spin>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import Header from './components/Header.vue'
import OverviewCard from './components/OverviewCard.vue'
import MarketCards from './components/MarketCards.vue'
import HoldingsCard from './components/HoldingsCard.vue'
import ActionStatusCard from './components/ActionStatusCard.vue'
import MarketModeCard from './components/MarketModeCard.vue'
import EtfPoolStatusCard from './components/EtfPoolStatusCard.vue'
import TacoHotTopicsCard from './components/TacoHotTopicsCard.vue'
import SignalPoolCard from './components/SignalPoolCard.vue'
import StockPoolCard from './components/StockPoolCard.vue'
import SignalReviewCard from './components/SignalReviewCard.vue'
import TimingReviewCard from './components/TimingReviewCard.vue'
import TimelineCard from './components/TimelineCard.vue'
import ReviewReportCard from './components/ReviewReportCard.vue'
import DynamicParamsCard from './components/DynamicParamsCard.vue'
import OverrideHistoryCard from './components/OverrideHistoryCard.vue'

import * as api from './api'

const loading = ref(false)
const overview = ref<any>({})
const market = ref<any>({})
const holdings = ref<any[]>([])
const signalPool = ref<any>({})
const stockPool = ref<any[]>([])
const signalReview = ref<any>({})
const timingReview = ref<any>({})
const timeline = ref<any[]>([])
const actionStatus = ref<any>({})
const etfPoolStatus = ref<any>({})
const reviewReport = ref<any>({})

const refreshAll = async () => {
  loading.value = true
  try {
    const [
      overviewData,
      marketData,
      holdingsData,
      signalPoolData,
      stockPoolData,
      signalReviewData,
      timingReviewData,
      timelineData,
      actionStatusData,
      etfPoolStatusData,
      reviewReportData,
    ] = await Promise.all([
      api.getOverview().catch(() => ({})),
      api.getMarket().catch(() => ({})),
      api.getHoldings().catch(() => []),
      api.getSignalPoolAll(80).catch(() => ({})),
      api.getStockPool(50).catch(() => []),
      api.getSignalReview(50).catch(() => ({})),
      api.getTimingReview(100).catch(() => ({})),
      api.getTimeline(80).catch(() => []),
      api.getRuntimeConfig().catch(() => ({})),
      api.getEtfPoolStatus().catch(() => ({})),
      api.getReviewReport().catch(() => ({})),
    ])

    overview.value = overviewData
    market.value = marketData
    holdings.value = holdingsData
    signalPool.value = signalPoolData
    stockPool.value = stockPoolData
    signalReview.value = signalReviewData
    timingReview.value = timingReviewData
    timeline.value = timelineData
    actionStatus.value = actionStatusData
    etfPoolStatus.value = etfPoolStatusData
    reviewReport.value = reviewReportData
  } finally {
    loading.value = false
  }
}

const refreshOverview = () => refreshAll()
const refreshDynamicParams = () => {}

const runAction = async (action: string) => {
  try {
    await api.runAction(action)
    await refreshAll()
  } catch (e) {
    console.error(e)
  }
}

onMounted(() => {
  refreshAll()
  setInterval(refreshAll, 30000)
})
</script>
