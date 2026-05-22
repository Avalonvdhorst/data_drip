class BackfillRunUpdates {
  constructor(element) {
    this.element = element
    this.backfillRunId = element.dataset.backfillRunId
    this.pollingInterval = null

    this.statusElement = element.querySelector('[data-target="status"]')
    this.processedCountElement = element.querySelector('[data-target="processedCount"]')
    this.totalCountElement = element.querySelector('[data-target="totalCount"]')
    this.batchesTableElement = element.querySelector('[data-target="batchesTable"]')
    this.progressBarElement = element.querySelector('[data-target="progressBar"]')
    this.runDurationElement = element.querySelector('[data-target="runDuration"]')
    this.averageBatchDurationElement = element.querySelector('[data-target="averageBatchDuration"]')
    this.elementsPerSecondElement = element.querySelector('[data-target="elementsPerSecond"]')
    this.actionButtonElement = element.querySelector('[data-target="actionButton"]')

    this.startPolling()
  }

  disconnect() {
    this.stopPolling()
  }

  startPolling() {
    // Poll immediately
    this.poll()

    // Then poll every 500ms
    this.pollingInterval = setInterval(() => {
      this.poll()
    }, 500)
  }

  stopPolling() {
    if (this.pollingInterval) {
      clearInterval(this.pollingInterval)
      this.pollingInterval = null
    }
  }

  async poll() {
    try {
      const response = await fetch(`/data_drip/backfill_runs/${this.backfillRunId}/updates`)
      if (!response.ok) {
        return
      }

      const data = await response.json()
      this.updateUI(data)

      // Stop polling if backfill is done
      if (data.status === 'completed' || data.status === 'failed' || data.status === 'stopped') {
        this.stopPolling()
      }
    } catch (error) {
      // Silently fail
    }
  }

  updateUI(data) {
    if (this.statusElement) {
      this.statusElement.innerHTML = data.status_html
    }

    if (this.processedCountElement) {
      this.processedCountElement.textContent = data.processed_count
    }

    if (this.totalCountElement) {
      this.totalCountElement.textContent = data.total_count
    }

    if (this.batchesTableElement) {
      this.batchesTableElement.innerHTML = data.batches_html
    }

    // Update progress bar
    if (this.progressBarElement) {
      const processedCount = parseInt(data.processed_count) || 0
      const totalCount = parseInt(data.total_count) || 0

      this.progressBarElement.setAttribute('value', processedCount)
      this.progressBarElement.setAttribute('max', totalCount)
      this.progressBarElement.value = processedCount
      this.progressBarElement.max = totalCount
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
