const POLL_INTERVAL_MS = 500

class BackfillRunUpdates {
  constructor(element) {
    this.element = element
    this.backfillRunId = element.dataset.backfillRunId
    this.pollTimeout = null
    this.abortController = null
    this.stopped = false

    this.statusElement = element.querySelector('[data-target="status"]')
    this.processedCountElements = element.querySelectorAll('[data-target="processedCount"]')
    this.totalCountElements = element.querySelectorAll('[data-target="totalCount"]')
    this.batchesTableElement = element.querySelector('[data-target="batchesTable"]')
    this.progressBarElement = element.querySelector('[data-target="progressBar"]')
    this.runDurationElement = element.querySelector('[data-target="runDuration"]')
    this.averageBatchDurationElement = element.querySelector('[data-target="averageBatchDuration"]')
    this.elementsPerSecondElement = element.querySelector('[data-target="elementsPerSecond"]')
    this.actionButtonElement = element.querySelector('[data-target="actionButton"]')
    this.progressBarTrackElement = element.querySelector('[data-target="progressBarTrack"]')

    this.poll()
  }

  disconnect() {
    this.stopped = true
    if (this.pollTimeout) {
      clearTimeout(this.pollTimeout)
      this.pollTimeout = null
    }
    if (this.abortController) {
      this.abortController.abort()
      this.abortController = null
    }
  }

  scheduleNext() {
    if (this.stopped) return
    this.pollTimeout = setTimeout(() => this.poll(), POLL_INTERVAL_MS)
  }

  async poll() {
    if (this.stopped) return

    this.abortController = new AbortController()
    try {
      const response = await fetch(
        `/data_drip/backfill_runs/${this.backfillRunId}/updates`,
        { signal: this.abortController.signal }
      )
      if (!response.ok) {
        this.scheduleNext()
        return
      }

      const data = await response.json()
      if (this.stopped) return
      this.updateUI(data)

      if (data.status === 'completed' || data.status === 'failed' || data.status === 'stopped') {
        this.stopped = true
        return
      }
    } catch (error) {
      if (error.name === 'AbortError') return
    } finally {
      this.abortController = null
    }

    this.scheduleNext()
  }

  updateUI(data) {
    if (this.statusElement) {
      this.statusElement.innerHTML = data.status_html
    }

    this.processedCountElements.forEach(el => { el.textContent = data.processed_count })
    this.totalCountElements.forEach(el => { el.textContent = data.total_count })

    if (this.batchesTableElement) {
      this.batchesTableElement.innerHTML = data.batches_html
    }

    // Update progress bar
    const processedCount = parseInt(data.processed_count) || 0
    const totalCount = parseInt(data.total_count) || 0
    const pct = totalCount > 0 ? (processedCount * 100 / totalCount) : 0

    if (this.progressBarElement) {
      if (this.progressBarElement.tagName === 'PROGRESS') {
        this.progressBarElement.value = processedCount
        this.progressBarElement.max = totalCount
      } else {
        this.progressBarElement.textContent = pct.toFixed(1) + '%'
      }
    }

    if (this.progressBarTrackElement) {
      this.progressBarTrackElement.style.width = pct.toFixed(1) + '%'
    }

    // Update insights
    if (data.insights) {
      if (this.runDurationElement) {
        this.runDurationElement.textContent = data.insights.run_duration
      }

      if (this.averageBatchDurationElement) {
        this.averageBatchDurationElement.textContent = data.insights.average_batch_duration
      }

      if (this.elementsPerSecondElement) {
        this.elementsPerSecondElement.textContent = data.insights.elements_per_second
      }
    }

    // Update action button
    if (this.actionButtonElement && data.action_button_html) {
      this.actionButtonElement.innerHTML = data.action_button_html
    }
  }
}

// Keep track of the current instance
let currentInstance = null

// Initialize on page load
function initBackfillRunUpdates() {
  // Stop any existing polling from previous page
  if (currentInstance) {
    currentInstance.disconnect()
    currentInstance = null
  }

  const element = document.querySelector('[data-backfill-run-updates]')
  if (element) {
    currentInstance = new BackfillRunUpdates(element)
  }
}

// Clean up when navigating away
function cleanupBackfillRunUpdates() {
  if (currentInstance) {
    currentInstance.disconnect()
    currentInstance = null
  }
}

document.addEventListener('DOMContentLoaded', initBackfillRunUpdates)
// Also handle Turbo navigation
document.addEventListener('turbo:load', initBackfillRunUpdates)
document.addEventListener('turbo:before-render', cleanupBackfillRunUpdates)
