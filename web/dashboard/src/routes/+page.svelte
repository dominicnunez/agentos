<script lang="ts">
  import { onMount } from 'svelte';
  import { api, connect, identifier } from '$lib/api';
  import '$lib/app.css';
  import type { Approval, CompletionReview, DashboardIdentity, IntentDraft, TaskView } from '$lib/types';

  type Section = 'overview' | 'work' | 'approvals' | 'reviews' | 'system';
  type IntentList = 'context' | 'deliverables' | 'completion_criteria' | 'constraints';

  let section: Section = 'overview';
  let identity: DashboardIdentity | null = null;
  let active: TaskView | null = null;
  let approvals: Approval[] = [];
  let reviews: CompletionReview[] = [];
  let selectedApproval: Approval | null = null;
  let selectedReview: CompletionReview | null = null;
  let task: TaskView | null = null;
  let taskID = '';
  let workText = '';
  let executionKind = '';
  let conversationID = '';
  let busy = false;
  let error = '';
  let notice = '';
  let approvalPhrase = '';
  let reviewPhrase = '';
  let revisionFeedback = '';
  let completionFields: Record<string, string> = {};
  let completionFiles: Record<string, File[]> = {};

  onMount(async () => {
    try {
      identity = await connect();
      await refresh();
    } catch (cause) {
      error = message(cause);
    }
  });

  async function refresh(): Promise<void> {
    if (!identity) return;
    error = '';
    const [activeResult, approvalResult, reviewResult] = await Promise.allSettled([
      api<TaskView>('/api/v1/user/intents/active'),
      api<{ approvals: Approval[] }>('/api/v1/control/approvals'),
      api<{ reviews: CompletionReview[] }>('/api/v1/user/reviews?limit=100')
    ]);
    active = activeResult.status === 'fulfilled' ? activeResult.value : null;
    if (active?.conversation_id) conversationID = active.conversation_id;
    approvals = approvalResult.status === 'fulfilled' ? approvalResult.value.approvals : [];
    reviews = reviewResult.status === 'fulfilled' ? reviewResult.value.reviews : [];
    selectedApproval = approvals.find((item) => item.approval_id === selectedApproval?.approval_id) ?? null;
    selectedReview = reviews.find((item) => item.review_id === selectedReview?.review_id) ?? null;
  }

  async function submitWork(): Promise<void> {
    const text = workText.trim();
    if (!text) return;
    await action(async () => {
      if (!conversationID) conversationID = identifier('user');
      active = await api<TaskView>('/api/v1/user/messages', {
        method: 'POST',
        body: JSON.stringify({
          conversation_id: conversationID,
          message_id: identifier('message'),
          text,
          ...(executionKind ? { execution_kind: executionKind } : {})
        })
      });
      workText = '';
      notice = active.prompt || 'The proposed work was updated.';
    });
  }

  async function confirmIntent(): Promise<void> {
    if (!active?.intent || !active.conversation_id) return;
    const draft = active.intent;
    const currentConversation = active.conversation_id;
    await action(async () => {
      active = await api<TaskView>(`/api/v1/user/intents/${encodeURIComponent(currentConversation)}/confirm`, {
        method: 'POST',
        body: JSON.stringify({ message_id: identifier('confirmation'), fingerprint: draft.fingerprint })
      });
      conversationID = '';
      notice = `Work ${active.work_id || ''} was created from the confirmed Intent.`;
      await refresh();
    });
  }

  async function findTask(): Promise<void> {
    const id = taskID.trim();
    if (!id) return;
    await action(async () => {
      task = await api<TaskView>(`/api/v1/user/tasks/${encodeURIComponent(id)}`);
      completionFields = {};
      completionFiles = {};
    });
  }

  async function submitCompletion(): Promise<void> {
    if (!task?.completion_contract) return;
    const currentTask = task;
    await action(async () => {
      const artifacts: { role: string; name: string; media_type: string; data: string }[] = [];
      for (const requirement of currentTask.completion_contract!.artifact_requirements ?? []) {
        for (const file of completionFiles[requirement.role] ?? []) {
          if (file.size < 1 || file.size > 16 * 1024 * 1024) {
            throw new Error(`${file.name} must contain 1 byte to 16 MiB.`);
          }
          artifacts.push({ role: requirement.role, name: file.name, media_type: file.type || 'application/octet-stream', data: await base64(file) });
        }
      }
      task = await api<TaskView>(`/api/v1/user/tasks/${encodeURIComponent(currentTask.task_id)}/completion`, {
        method: 'POST',
        body: JSON.stringify({ message_id: identifier('completion'), fields: completionFields, artifacts })
      });
      notice = 'Completion evidence was submitted for deterministic validation.';
      await refresh();
    });
  }

  async function decideApproval(decision: 'APPROVE' | 'DENY'): Promise<void> {
    if (!selectedApproval) return;
    const approval = selectedApproval;
    const expected = decision === 'APPROVE' ? `APPROVE ${approval.effect_fingerprint.slice(0, 12)}` : 'DENY';
    if (approvalPhrase.trim() !== expected) {
      error = `Type ${expected} exactly.`;
      return;
    }
    await action(async () => {
      let current = approval;
      const body = JSON.stringify({ effect_fingerprint: current.effect_fingerprint });
      if (current.status === 'PENDING' || current.status === 'NOTIFIED') {
        current = await api<Approval>(`/api/v1/control/approvals/${encodeURIComponent(current.approval_id)}/acknowledge`, { method: 'POST', body });
      }
      if (current.status === 'ACKNOWLEDGED') {
        current = await api<Approval>(`/api/v1/control/approvals/${encodeURIComponent(current.approval_id)}/begin`, { method: 'POST', body });
      }
      selectedApproval = await api<Approval>(`/api/v1/control/approvals/${encodeURIComponent(current.approval_id)}/decision`, {
        method: 'POST',
        body: JSON.stringify({ effect_fingerprint: current.effect_fingerprint, decision })
      });
      approvalPhrase = '';
      notice = `Exact effect ${decision === 'APPROVE' ? 'approved' : 'denied'}.`;
      await refresh();
    });
  }

  async function decideReview(decision: 'APPROVE' | 'REJECT' | 'REVISE'): Promise<void> {
    if (!selectedReview) return;
    const review = selectedReview;
    const expected = `${decision} ${review.fingerprint.slice(0, 12)}`;
    if (reviewPhrase.trim() !== expected) {
      error = `Type ${expected} exactly.`;
      return;
    }
    if (decision === 'REVISE' && !revisionFeedback.trim()) {
      error = 'Revision feedback is required.';
      return;
    }
    await action(async () => {
      selectedReview = await api<CompletionReview>(`/api/v1/user/reviews/${encodeURIComponent(review.task_id)}`, {
        method: 'POST',
        body: JSON.stringify({ review_id: review.review_id, fingerprint: review.fingerprint, decision, feedback: revisionFeedback.trim() || undefined })
      });
      reviewPhrase = '';
      revisionFeedback = '';
      notice = `Completion evidence marked ${decision.toLowerCase()}.`;
      await refresh();
    });
  }

  async function action(run: () => Promise<void>): Promise<void> {
    busy = true;
    error = '';
    notice = '';
    try {
      await run();
    } catch (cause) {
      error = message(cause);
    } finally {
      busy = false;
    }
  }

  function setFiles(role: string, event: Event): void {
    const input = event.currentTarget as HTMLInputElement;
    completionFiles = { ...completionFiles, [role]: Array.from(input.files ?? []) };
  }

  async function base64(file: File): Promise<string> {
    const bytes = new Uint8Array(await file.arrayBuffer());
    let binary = '';
    const chunk = 32 * 1024;
    for (let offset = 0; offset < bytes.length; offset += chunk) {
      binary += String.fromCharCode(...bytes.subarray(offset, offset + chunk));
    }
    return btoa(binary);
  }

  function message(cause: unknown): string {
    return cause instanceof Error ? cause.message : 'The request failed.';
  }

  function values(draft: IntentDraft, key: IntentList) {
    return draft[key] ?? [];
  }
</script>

<svelte:head>
  <title>Agent OS</title>
  <meta name="description" content="Agent OS organization dashboard" />
</svelte:head>

<div class="shell">
  <aside>
    <div class="brand"><span class="mark">AO</span><div><strong>Agent OS</strong><small>Organization control</small></div></div>
    <nav aria-label="Dashboard">
      {#each [['overview','Overview'],['work','Work'],['approvals','Approvals'],['reviews','Reviews'],['system','System']] as item}
        <button class:active={section === item[0]} onclick={() => section = item[0] as Section}><span class="nav-dot"></span>{item[1]}</button>
      {/each}
    </nav>
    <div class="identity"><span class:online={Boolean(identity)}></span><div><strong>{identity?.organization ?? 'Not connected'}</strong><small>{identity ? `${identity.mode} installation` : 'local session required'}</small></div></div>
  </aside>

  <main>
    <header><div><p class="eyebrow">Artificial organization</p><h1>{section === 'overview' ? 'Command center' : section[0].toUpperCase() + section.slice(1)}</h1></div><button class="quiet" onclick={refresh} disabled={!identity || busy}>Refresh</button></header>
    {#if error}<div class="banner error" role="alert">{error}</div>{/if}
    {#if notice}<div class="banner notice" role="status">{notice}</div>{/if}

    {#if section === 'overview'}
      <section class="metrics">
        <button onclick={() => section='work'}><span>Active intake</span><strong>{active ? '1' : '0'}</strong><small>{active?.state ?? 'No open conversation'}</small></button>
        <button onclick={() => section='approvals'}><span>Approvals</span><strong>{approvals.length}</strong><small>Exact effects awaiting a decision</small></button>
        <button onclick={() => section='reviews'}><span>Completion reviews</span><strong>{reviews.length}</strong><small>Evidence awaiting judgment</small></button>
      </section>
      <section class="panel mission">
        <div><p class="eyebrow">Current organization</p><h2>{identity?.organization ?? 'Connect to Agent OS'}</h2><p>Durable work enters through one governed intake boundary. Language can propose work; it cannot grant authority or prove completion.</p></div>
        <div class="flow"><span>Intent</span><i></i><span>Plan</span><i></i><span>Work</span><i></i><span>Evidence</span></div>
      </section>
      <section class="grid two">
        <div class="panel"><div class="panel-title"><h2>Continue work</h2><button class="text" onclick={() => section='work'}>Open work</button></div>{#if active}<span class="status">{active.state}</span><h3>{active.intent?.objective ?? active.prompt ?? 'Intake conversation'}</h3><p class="mono">{active.conversation_id}</p>{:else}<div class="empty">No unfinished intake conversation.</div>{/if}</div>
        <div class="panel"><div class="panel-title"><h2>Governance queue</h2></div><div class="queue"><div><strong>{approvals.length}</strong><span>effect decisions</span></div><div><strong>{reviews.length}</strong><span>evidence reviews</span></div></div></div>
      </section>
    {:else if section === 'work'}
      <section class="grid work-grid">
        <div class="panel composer"><p class="eyebrow">Natural-language intake</p><h2>{active ? 'Continue the conversation' : 'What should the organization accomplish?'}</h2><textarea bind:value={workText} rows="7" placeholder="Describe the outcome, relevant context, constraints, and anything only you can provide."></textarea><label>Execution<select bind:value={executionKind} disabled={Boolean(active)}><option value="">Automatic</option><option value="HUMAN">User task</option></select><small>Automatic prefers deterministic work and uses an Agent only when justified.</small></label><div class="actions"><button class="primary" onclick={submitWork} disabled={busy || !identity || !workText.trim()}>Send</button><small>The model proposes a bounded Intent. Nothing starts until you confirm the exact review.</small></div></div>
        <div class="panel intent"><div class="panel-title"><div><p class="eyebrow">Intent contract</p><h2>Review before work begins</h2></div>{#if active?.state}<span class="status">{active.state}</span>{/if}</div>
          {#if active?.intent}
            <h3>{active.intent.objective}</h3>
            <dl><div><dt>Mode</dt><dd>{active.intent.mode}</dd></div>{#if active.intent.goal}<div><dt>Goal</dt><dd>{active.intent.goal.value}</dd></div>{/if}{#if active.intent.replaces_work}<div><dt>Replaces Work</dt><dd>{active.intent.replaces_work.value}</dd></div>{/if}</dl>
            {#each [['Context','context'],['Deliverables','deliverables'],['Done when','completion_criteria'],['Requirements','constraints']] as group}
              {#if values(active.intent, group[1] as IntentList).length}<h4>{group[0]}</h4><ul>{#each values(active.intent, group[1] as IntentList) as value}<li>{value.value}</li>{/each}</ul>{/if}
            {/each}
            {#if active.intent.consequence_candidates?.length}<h4>Potential task boundaries</h4><ul>{#each active.intent.consequence_candidates as boundary}<li>{boundary}</li>{/each}</ul>{/if}
            <div class="fingerprint"><span>Intent v{active.intent.version}</span><code>{active.intent.fingerprint}</code></div>
            <button class="primary wide" onclick={confirmIntent} disabled={busy || active.state !== 'AWAITING_CONFIRMATION'}>Confirm exact Intent</button>
            <p class="boundary-note">Confirming starts planning. It does not approve financial, public, destructive, privileged, legal, deployment, or other consequential effects.</p>
          {:else if active}<div class="empty"><p>{active.prompt || 'More information is required before an Intent can be reviewed.'}</p></div>{:else}<div class="empty">Submit an outcome to begin a durable intake conversation.</div>{/if}
        </div>
      </section>
      <section class="panel task-lookup"><div><p class="eyebrow">Durable status</p><h2>Find a Task</h2></div><div class="inline"><input bind:value={taskID} placeholder="task-id" /><button onclick={findTask} disabled={busy || !taskID.trim()}>Open</button></div>
        {#if task}<div class="task"><div><span class="status">{task.state}</span><h3>{task.task_id}</h3>{#if task.work_id}<p class="mono">Work {task.work_id}</p>{/if}{#if task.result}<p>{task.result}</p>{/if}</div>
          {#if task.completion_contract}<form onsubmit={(event) => { event.preventDefault(); submitCompletion(); }}><h4>Required completion evidence</h4>{#each task.completion_contract.required_fields ?? [] as field}<label>{field.name}<small>{field.description}</small><textarea required minlength={field.min_bytes} maxlength={field.max_bytes} value={completionFields[field.name] ?? ''} oninput={(event) => completionFields = {...completionFields, [field.name]: event.currentTarget.value}}></textarea></label>{/each}{#each task.completion_contract.artifact_requirements ?? [] as requirement}<label>{requirement.role}<small>{requirement.min_count}–{requirement.max_count} files; {requirement.media_types.join(', ')}</small><input type="file" required={requirement.min_count > 0} multiple={requirement.max_count > 1} accept={requirement.media_types.join(',')} onchange={(event) => setFiles(requirement.role, event)} /></label>{/each}<button class="primary" type="submit" disabled={busy}>Submit complete evidence</button></form>{/if}
        </div>{/if}
      </section>
    {:else if section === 'approvals'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">Consequential effects</p><h2>Pending approvals</h2></div><span class="count">{approvals.length}</span></div>{#if approvals.length}{#each approvals as approval}<button class:selected={selectedApproval?.approval_id === approval.approval_id} onclick={() => {selectedApproval=approval; approvalPhrase='';}}><div><strong>{approval.action}</strong><span>{approval.resource}</span></div><div><span class="risk">{approval.risk}</span><small>{approval.boundary}</small></div></button>{/each}{:else}<div class="empty">No exact effects are awaiting a decision.</div>{/if}</div>
        <div class="panel detail">{#if selectedApproval}<p class="eyebrow">Exact proposed effect</p><h2>{selectedApproval.canonical_effect_descriptor}</h2><dl><div><dt>Action</dt><dd>{selectedApproval.action}</dd></div><div><dt>Resource</dt><dd>{selectedApproval.resource}</dd></div><div><dt>Scope</dt><dd>{selectedApproval.scope}</dd></div><div><dt>Boundary</dt><dd>{selectedApproval.boundary}</dd></div><div><dt>Risk</dt><dd>{selectedApproval.risk}</dd></div><div><dt>Single use</dt><dd>{selectedApproval.single_use ? 'Yes' : 'No'}</dd></div></dl>{#if Object.keys(selectedApproval.effect_arguments).length}<h4>Arguments</h4><pre>{JSON.stringify(selectedApproval.effect_arguments, null, 2)}</pre>{/if}<div class="fingerprint"><span>Effect fingerprint</span><code>{selectedApproval.effect_fingerprint}</code></div><label>Type <code>APPROVE {selectedApproval.effect_fingerprint.slice(0,12)}</code> or <code>DENY</code><input bind:value={approvalPhrase} autocomplete="off" /></label><div class="actions"><button class="danger" onclick={() => decideApproval('DENY')} disabled={busy}>Deny</button><button class="primary" onclick={() => decideApproval('APPROVE')} disabled={busy}>Approve exact effect</button></div>{:else}<div class="empty">Select an approval to inspect the immutable effect details.</div>{/if}</div>
      </section>
    {:else if section === 'reviews'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">Completion evidence</p><h2>Review queue</h2></div><span class="count">{reviews.length}</span></div>{#if reviews.length}{#each reviews as review}<button class:selected={selectedReview?.review_id === review.review_id} onclick={() => {selectedReview=review; reviewPhrase=''; revisionFeedback='';}}><div><strong>{review.objective}</strong><span>{review.task_id}</span></div><span class="status">{review.state}</span></button>{/each}{:else}<div class="empty">No completion evidence is awaiting judgment.</div>{/if}</div>
        <div class="panel detail">{#if selectedReview}<p class="eyebrow">Candidate result</p><h2>{selectedReview.objective}</h2><blockquote>{selectedReview.candidate_result ?? selectedReview.result ?? 'No text result supplied.'}</blockquote><h4>Done when</h4><ul>{#each selectedReview.criteria as criterion}<li>{criterion.description}</li>{/each}</ul><h4>Evidence references</h4><ul class="mono">{#each selectedReview.evidence_refs as ref}<li>{ref}</li>{/each}</ul><div class="fingerprint"><span>Evidence fingerprint</span><code>{selectedReview.fingerprint}</code></div><p class="boundary-note">This judgment verifies the recorded candidate only. It does not approve any consequential effect.</p><label>Type <code>APPROVE {selectedReview.fingerprint.slice(0,12)}</code>, <code>REJECT {selectedReview.fingerprint.slice(0,12)}</code>, or <code>REVISE {selectedReview.fingerprint.slice(0,12)}</code><input bind:value={reviewPhrase} autocomplete="off" /></label>{#if reviewPhrase.startsWith('REVISE')}<label>Revision feedback<textarea bind:value={revisionFeedback} required></textarea></label>{/if}<div class="actions three"><button class="danger" onclick={() => decideReview('REJECT')} disabled={busy}>Reject</button><button onclick={() => decideReview('REVISE')} disabled={busy}>Request revision</button><button class="primary" onclick={() => decideReview('APPROVE')} disabled={busy}>Approve evidence</button></div>{:else}<div class="empty">Select a review to compare the candidate result with its exact completion contract.</div>{/if}</div>
      </section>
    {:else}
      <section class="grid two"><div class="panel"><p class="eyebrow">Local boundary</p><h2>Dashboard session</h2><dl><div><dt>Organization</dt><dd>{identity?.organization ?? 'Unavailable'}</dd></div><div><dt>Install mode</dt><dd>{identity?.mode ?? 'Unavailable'}</dd></div><div><dt>Agent OS</dt><dd>{identity?.version ?? 'Unavailable'}</dd></div><div><dt>Expires</dt><dd>{identity?.session_expires_at ?? 'Unavailable'}</dd></div></dl></div><div class="panel"><p class="eyebrow">Diagnostics</p><h2>Read-only system checks</h2><p>Use <code>agentos doctor</code> for configuration, credential, service, private-gateway, and SQLite integrity checks.</p><pre>agentos doctor
sudo agentos doctor</pre></div></section>
    {/if}
  </main>
</div>
