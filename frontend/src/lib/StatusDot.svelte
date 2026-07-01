<script lang="ts">
  // Small colored dot for a run status. Pass `label` to expose it to
  // assistive tech (role="img"); omit it when the status text is rendered
  // right next to the dot (the dot is then decorative).
  let { status, label }: { status: string; label?: string } = $props();
</script>

{#if label}
  <span
    class="status-dot status-{status}"
    role="img"
    aria-label={label}
    title={status}
  ></span>
{:else}
  <span class="status-dot status-{status}" aria-hidden="true"></span>
{/if}

<style>
  .status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
    background: #9aa0a6; /* fallback for unknown statuses, e.g. "abandoned" */
  }
  .status-success {
    background: #34a853;
  }
  .status-error {
    background: #ea4335;
  }
  .status-running {
    background: #fbbc04;
  }
</style>
