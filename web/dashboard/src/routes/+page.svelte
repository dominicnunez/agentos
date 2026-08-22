<script lang="ts">
  import { onMount } from 'svelte';
  import { APIError, api, connect, identifier } from '$lib/api';
  import { confirmationMessageID, hasRetryableIntentConfirmation, loadAllCompletionReviews, safeDisplay, sameCompletionContract, snapshotCompletionEvidence, validateArtifactSelections, validateCompletionFields } from '$lib/governance';
  import '$lib/app.css';
  import type { Approval, CompletionReview, CompletionReviewPage, DashboardIdentity, IntentDraft, TaskView } from '$lib/types';

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
  let pendingWorkMessageID = '';
  let pendingWorkKey = '';
  let completionMessageID = '';
  let taskInput = '';
  let pendingTaskInputMessageID = '';
  let pendingTaskInputKey = '';
  let refreshGeneration = 0;

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
    const generation = ++refreshGeneration;
    const displayedTask = task;
    const displayedActive = active;
    error = '';
    const [activeResult, approvalResult, reviewResult, taskResult] = await Promise.allSettled([
      loadActiveIntent(),
      api<{ approvals: Approval[] }>('/api/v1/control/approvals'),
      loadReviews(),
      displayedTask ? api<TaskView>(`/api/v1/user/tasks/${encodeURIComponent(displayedTask.task_id)}`) : Promise.resolve(null)
    ]);
    if (generation !== refreshGeneration) return;
    const failures = [activeResult, approvalResult, reviewResult, taskResult]
      .filter((result) => result.status === 'rejected')
      .map((result) => message((result as PromiseRejectedResult).reason));
    if (activeResult.status === 'fulfilled') {
      if (activeResult.value) {
        active = activeResult.value;
        conversationID = activeResult.value.conversation_id;
      } else if (!hasRetryableIntentConfirmation(displayedActive)) {
        active = null;
        conversationID = '';
      }
    }
    if (approvalResult.status === 'fulfilled') approvals = approvalResult.value.approvals;
    if (reviewResult.status === 'fulfilled') reviews = reviewResult.value;
    if (taskResult.status === 'fulfilled' && taskResult.value) {
      if (!sameCompletionContract(displayedTask?.completion_contract, taskResult.value.completion_contract)) clearCompletionEvidence();
      task = taskResult.value;
      taskID = taskResult.value.task_id;
      if (taskResult.value.state !== 'INPUT_REQUIRED') clearTaskInput();
    }
    if (failures.length) error = `Dashboard refresh failed; previously loaded governance data was preserved. ${failures.join(' ')}`;
    selectedApproval = approvals.find((item) => item.approval_id === selectedApproval?.approval_id) ?? null;
    selectedReview = reviews.find((item) => item.review_id === selectedReview?.review_id) ?? null;
  }

  async function loadActiveIntent(): Promise<TaskView | null> {
    try {
      return await api<TaskView>('/api/v1/user/intents/active');
    } catch (cause) {
      if (cause instanceof APIError && cause.status === 404) return null;
      throw cause;
    }
  }

  async function loadReviews(): Promise<CompletionReview[]> {
    return loadAllCompletionReviews((after) => api<CompletionReviewPage>(`/api/v1/user/reviews?limit=100${after ? `&after=${encodeURIComponent(after)}` : ''}`));
  }

  async function submitWork(): Promise<void> {
    const text = workText.trim();
    if (!text) return;
    if (!conversationID) conversationID = identifier('user');
    const currentConversation = conversationID;
    const requestKey = JSON.stringify([currentConversation, text, executionKind]);
    if (!pendingWorkMessageID || pendingWorkKey !== requestKey) {
      pendingWorkMessageID = identifier('message');
      pendingWorkKey = requestKey;
    }
    const messageID = pendingWorkMessageID;
    await action(async () => {
      active = await api<TaskView>('/api/v1/user/messages', {
        method: 'POST',
        body: JSON.stringify({
          conversation_id: currentConversation,
          message_id: messageID,
          text,
          ...(executionKind ? { execution_kind: executionKind } : {})
        })
      });
      pendingWorkMessageID = '';
      pendingWorkKey = '';
      if (workText.trim() === text) workText = '';
      notice = active.prompt || 'The proposed work was updated.';
    });
  }

  async function confirmIntent(): Promise<void> {
    if (!active?.intent || !active.conversation_id) return;
    const draft = active.intent;
    const currentConversation = active.conversation_id;
    await action(async () => {
      const confirmed = await api<TaskView>(`/api/v1/user/intents/${encodeURIComponent(currentConversation)}/confirm`, {
        method: 'POST',
        body: JSON.stringify({ message_id: confirmationMessageID(draft.fingerprint), fingerprint: draft.fingerprint })
      });
      clearCompletionEvidence();
      executionKind = '';
      pendingWorkMessageID = '';
      pendingWorkKey = '';
      active = null;
      task = confirmed;
      taskID = confirmed.task_id;
      clearTaskInput();
      conversationID = '';
      notice = `Task ${confirmed.task_id} for Work ${confirmed.work_id || ''} was created from the confirmed Intent.`;
      await refresh();
    });
  }

  async function findTask(): Promise<void> {
    const id = taskID.trim();
    if (!id) return;
    await action(async () => {
      task = await api<TaskView>(`/api/v1/user/tasks/${encodeURIComponent(id)}`);
      clearCompletionEvidence();
      clearTaskInput();
    });
  }

  async function submitTaskInput(): Promise<void> {
    if (!task?.conversation_id || task.state !== 'INPUT_REQUIRED' || task.completion_contract) return;
    const text = taskInput.trim();
    if (!text) return;
    const currentTask = task;
    const requestKey = JSON.stringify([currentTask.conversation_id, currentTask.task_id, text]);
    if (!pendingTaskInputMessageID || pendingTaskInputKey !== requestKey) {
      pendingTaskInputMessageID = identifier('input');
      pendingTaskInputKey = requestKey;
    }
    const messageID = pendingTaskInputMessageID;
    await action(async () => {
      task = await api<TaskView>('/api/v1/user/messages', {
        method: 'POST',
        body: JSON.stringify({ conversation_id: currentTask.conversation_id, message_id: messageID, text })
      });
      taskID = task.task_id;
      pendingTaskInputMessageID = '';
      pendingTaskInputKey = '';
      if (taskInput.trim() === text) taskInput = '';
      notice = task.prompt || 'The requested input was recorded and work resumed.';
      await refresh();
    });
  }

  async function submitCompletion(): Promise<void> {
    if (!task?.completion_contract) return;
    const currentTask = task;
    const evidence = snapshotCompletionEvidence(completionFields, completionFiles);
    if (!completionMessageID) completionMessageID = identifier('completion');
    const messageID = completionMessageID;
    await action(async () => {
      const fieldError = validateCompletionFields(currentTask.completion_contract!.required_fields ?? [], evidence.fields);
      if (fieldError) throw new Error(fieldError);
      const selectionError = validateArtifactSelections(currentTask.completion_contract!.artifact_requirements ?? [], evidence.files);
      if (selectionError) throw new Error(selectionError);
      const artifacts: { role: string; name: string; media_type: string; data: string }[] = [];
      for (const requirement of currentTask.completion_contract!.artifact_requirements ?? []) {
        for (const file of evidence.files[requirement.role] ?? []) {
          artifacts.push({ role: requirement.role, name: file.name, media_type: '', data: await base64(file) });
        }
      }
      task = await api<TaskView>(`/api/v1/user/tasks/${encodeURIComponent(currentTask.task_id)}/completion`, {
        method: 'POST',
        body: JSON.stringify({ message_id: messageID, fields: evidence.fields, artifacts })
      });
      completionMessageID = '';
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
    refreshGeneration += 1;
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
    completionMessageID = '';
    completionFiles = { ...completionFiles, [role]: Array.from(input.files ?? []) };
  }

  function setCompletionField(name: string, value: string): void {
    completionMessageID = '';
    completionFields = { ...completionFields, [name]: value };
  }

  function clearCompletionEvidence(): void {
    completionFields = {};
    completionFiles = {};
    completionMessageID = '';
  }

  function clearTaskInput(): void {
    taskInput = '';
    pendingTaskInputMessageID = '';
    pendingTaskInputKey = '';
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
    return cause instanceof Error ? safeDisplay(cause.message) : 'The request failed.';
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
    <div class="identity"><span class:online={Boolean(identity)}></span><div><strong>{safeDisplay(identity?.organization ?? 'Not connected')}</strong><small>{identity ? `${safeDisplay(identity.mode)} installation` : 'local session required'}</small></div></div>
  </aside>

  <main>
    <header><div><p class="eyebrow">Artificial organization</p><h1>{section === 'overview' ? 'Command center' : section[0].toUpperCase() + section.slice(1)}</h1></div><button class="quiet" onclick={refresh} disabled={!identity || busy}>Refresh</button></header>
    {#if error}<div class="banner error" role="alert">{safeDisplay(error)}</div>{/if}
    {#if notice}<div class="banner notice" role="status">{safeDisplay(notice)}</div>{/if}

    {#if section === 'overview'}
      <section class="metrics">
        <button onclick={() => section='work'}><span>Active intake</span><strong>{active ? '1' : '0'}</strong><small>{safeDisplay(active?.state ?? 'No open conversation')}</small></button>
        <button onclick={() => section='approvals'}><span>Approvals</span><strong>{approvals.length}</strong><small>Exact effects awaiting a decision</small></button>
        <button onclick={() => section='reviews'}><span>Completion reviews</span><strong>{reviews.length}</strong><small>Evidence awaiting judgment</small></button>
      </section>
      <section class="panel mission">
        <div><p class="eyebrow">Current organization</p><h2>{safeDisplay(identity?.organization ?? 'Connect to Agent OS')}</h2><p>Durable work enters through one governed intake boundary. Language can propose work; it cannot grant authority or prove completion.</p></div>
        <div class="flow"><span>Intent</span><i></i><span>Plan</span><i></i><span>Work</span><i></i><span>Evidence</span></div>
      </section>
      <section class="grid two">
        <div class="panel"><div class="panel-title"><h2>Continue work</h2><button class="text" onclick={() => section='work'}>Open work</button></div>{#if active}<span class="status">{safeDisplay(active.state)}</span><h3>{safeDisplay(active.intent?.objective ?? active.prompt ?? 'Intake conversation')}</h3><p class="mono">{safeDisplay(active.conversation_id)}</p>{:else}<div class="empty">No unfinished intake conversation.</div>{/if}</div>
        <div class="panel"><div class="panel-title"><h2>Governance queue</h2></div><div class="queue"><div><strong>{approvals.length}</strong><span>effect decisions</span></div><div><strong>{reviews.length}</strong><span>evidence reviews</span></div></div></div>
      </section>
    {:else if section === 'work'}
      <section class="grid work-grid">
        <div class="panel composer"><p class="eyebrow">Natural-language intake</p><h2>{active ? 'Continue the conversation' : 'What should the organization accomplish?'}</h2><textarea bind:value={workText} disabled={busy} rows="7" placeholder="Describe the outcome, relevant context, constraints, and anything only you can provide."></textarea><label>Execution<select bind:value={executionKind} disabled={busy || Boolean(active)}><option value="">Automatic</option><option value="HUMAN">User task</option></select><small>Automatic prefers deterministic work and uses an Agent only when justified.</small></label><div class="actions"><button class="primary" onclick={submitWork} disabled={busy || !identity || !workText.trim()}>Send</button><small>The model proposes a bounded Intent. Nothing starts until you confirm the exact review.</small></div></div>
        <div class="panel intent"><div class="panel-title"><div><p class="eyebrow">Intent contract</p><h2>Review before work begins</h2></div>{#if active?.state}<span class="status">{safeDisplay(active.state)}</span>{/if}</div>
          {#if active?.intent}
            {#if active.prompt}<div class="banner notice governed-text" role="status"><strong>More information required</strong><br />{safeDisplay(active.prompt)}</div>{/if}
            <h3>{safeDisplay(active.intent.objective)}</h3>
            <dl><div><dt>Mode</dt><dd>{safeDisplay(active.intent.mode)}</dd></div>{#if active.intent.requested_execution_kind}<div><dt>Requested execution</dt><dd>{safeDisplay(active.intent.requested_execution_kind)}</dd></div>{/if}{#if active.intent.goal}<div><dt>Goal</dt><dd>{safeDisplay(active.intent.goal.value)}</dd></div>{/if}{#if active.intent.replaces_work}<div><dt>Replaces Work</dt><dd>{safeDisplay(active.intent.replaces_work.value)}</dd></div>{/if}</dl>
            {#each [['Context','context'],['Deliverables','deliverables'],['Done when','completion_criteria'],['Requirements','constraints']] as group}
              {#if values(active.intent, group[1] as IntentList).length}<h4>{group[0]}</h4><ul>{#each values(active.intent, group[1] as IntentList) as value}<li>{safeDisplay(value.value)}</li>{/each}</ul>{/if}
            {/each}
            {#if active.intent.resolved_decisions?.length}<h4>Resolved decisions</h4><ul>{#each active.intent.resolved_decisions as decision}<li><strong>{safeDisplay(decision.subject)}:</strong> {safeDisplay(decision.value)}</li>{/each}</ul>{/if}
            {#if active.intent.consequence_candidates?.length}<h4>Potential task boundaries</h4><ul>{#each active.intent.consequence_candidates as boundary}<li>{safeDisplay(boundary)}</li>{/each}</ul>{/if}
            <div class="fingerprint"><span>Intent v{active.intent.version}</span><code>{active.intent.fingerprint}</code></div>
            <button class="primary wide" onclick={confirmIntent} disabled={busy || active.state !== 'AWAITING_CONFIRMATION'}>Confirm exact Intent</button>
            <p class="boundary-note">Confirming starts planning. It does not approve financial, public, destructive, privileged, legal, deployment, or other consequential effects.</p>
          {:else if active}<div class="empty"><p>{safeDisplay(active.prompt || 'More information is required before an Intent can be reviewed.')}</p></div>{:else}<div class="empty">Submit an outcome to begin a durable intake conversation.</div>{/if}
        </div>
      </section>
      <section class="panel task-lookup"><div><p class="eyebrow">Durable status</p><h2>Find a Task</h2></div><div class="inline"><input bind:value={taskID} placeholder="task-id" /><button onclick={findTask} disabled={busy || !taskID.trim()}>Open</button></div>
        {#if task}<div class="task"><div><span class="status">{safeDisplay(task.state)}</span><h3>{safeDisplay(task.task_id)}</h3>{#if task.work_id}<p class="mono">Work {safeDisplay(task.work_id)}</p>{/if}{#if task.mode}<p>Mode: <strong>{safeDisplay(task.mode)}</strong></p>{/if}{#if task.trust_label}<p class="risk">Trust: {safeDisplay(task.trust_label)}</p>{/if}{#if task.prompt}<p class="boundary-note governed-text">{safeDisplay(task.prompt)}</p>{/if}{#if task.result}<p class="governed-text">{safeDisplay(task.result)}</p>{/if}</div>
          {#if task.completion_contract}{#key `${task.task_id}:${task.completion_contract.task_version}`}<form onsubmit={(event) => { event.preventDefault(); submitCompletion(); }}><h4>Required completion evidence</h4>{#each task.completion_contract.required_fields ?? [] as field}<label>{safeDisplay(field.name)}<small>{safeDisplay(field.description)}; {field.min_bytes} to {field.max_bytes} UTF-8 bytes</small><textarea required disabled={busy} value={completionFields[field.name] ?? ''} oninput={(event) => setCompletionField(field.name, event.currentTarget.value)}></textarea></label>{/each}{#each task.completion_contract.artifact_requirements ?? [] as requirement}<label>{safeDisplay(requirement.role)}<small>{requirement.min_count} to {requirement.max_count} files; {requirement.media_types.map(safeDisplay).join(', ')}</small><input type="file" disabled={busy} required={requirement.min_count > 0} multiple={requirement.max_count > 1} accept={requirement.media_types.join(',')} onchange={(event) => setFiles(requirement.role, event)} /></label>{/each}<button class="primary" type="submit" disabled={busy}>Submit complete evidence</button></form>{/key}{:else if task.state === 'INPUT_REQUIRED' && task.conversation_id}<form onsubmit={(event) => { event.preventDefault(); submitTaskInput(); }}><h4>Provide requested input</h4><label>Response<textarea bind:value={taskInput} disabled={busy} required placeholder="Provide the information requested above."></textarea></label><button class="primary" type="submit" disabled={busy || !taskInput.trim()}>Continue Task</button></form>{/if}
        </div>{/if}
      </section>
    {:else if section === 'approvals'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">Consequential effects</p><h2>Pending approvals</h2></div><span class="count">{approvals.length}</span></div>{#if approvals.length}{#each approvals as approval}<button class:selected={selectedApproval?.approval_id === approval.approval_id} onclick={() => {selectedApproval=approval; approvalPhrase='';}}><div><strong>{safeDisplay(approval.action)}</strong><span>{safeDisplay(approval.resource)}</span></div><div><span class="risk">{safeDisplay(approval.risk)}</span><small>{safeDisplay(approval.boundary)}</small></div></button>{/each}{:else}<div class="empty">No exact effects are awaiting a decision.</div>{/if}</div>
        <div class="panel detail">{#if selectedApproval}<p class="eyebrow">Exact proposed effect</p><h2>{safeDisplay(selectedApproval.canonical_effect_descriptor)}</h2><dl><div><dt>Action</dt><dd>{safeDisplay(selectedApproval.action)}</dd></div><div><dt>Resource</dt><dd>{safeDisplay(selectedApproval.resource)}</dd></div><div><dt>Scope</dt><dd>{safeDisplay(selectedApproval.scope)}</dd></div><div><dt>Boundary</dt><dd>{safeDisplay(selectedApproval.boundary)}</dd></div><div><dt>Risk</dt><dd>{safeDisplay(selectedApproval.risk)}</dd></div><div><dt>Urgency</dt><dd>{safeDisplay(selectedApproval.urgency)}</dd></div><div><dt>Expires</dt><dd>{safeDisplay(selectedApproval.expires_at ?? 'No expiry recorded')}</dd></div><div><dt>Single use</dt><dd>{selectedApproval.single_use ? 'Yes' : 'No'}</dd></div></dl>{#if Object.keys(selectedApproval.effect_arguments).length}<h4>Arguments</h4><pre>{safeDisplay(JSON.stringify(selectedApproval.effect_arguments, null, 2))}</pre>{/if}<div class="fingerprint"><span>Effect fingerprint</span><code>{selectedApproval.effect_fingerprint}</code></div><label>Type <code>APPROVE {selectedApproval.effect_fingerprint.slice(0,12)}</code> or <code>DENY</code><input bind:value={approvalPhrase} autocomplete="off" /></label><div class="actions"><button class="danger" onclick={() => decideApproval('DENY')} disabled={busy}>Deny</button><button class="primary" onclick={() => decideApproval('APPROVE')} disabled={busy}>Approve exact effect</button></div>{:else}<div class="empty">Select an approval to inspect the immutable effect details.</div>{/if}</div>
      </section>
    {:else if section === 'reviews'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">Completion evidence</p><h2>Review queue</h2></div><span class="count">{reviews.length}</span></div>{#if reviews.length}{#each reviews as review}<button class:selected={selectedReview?.review_id === review.review_id} onclick={() => {selectedReview=review; reviewPhrase=''; revisionFeedback='';}}><div><strong>{safeDisplay(review.objective)}</strong><span>{safeDisplay(review.task_id)}</span></div><span class="status">{safeDisplay(review.state)}</span></button>{/each}{:else}<div class="empty">No completion evidence is awaiting judgment.</div>{/if}</div>
        <div class="panel detail">{#if selectedReview}<p class="eyebrow">Candidate result</p><h2>{safeDisplay(selectedReview.objective)}</h2><blockquote>{safeDisplay(selectedReview.candidate_result ?? selectedReview.result ?? 'No text result supplied.')}</blockquote><h4>Done when</h4><ul>{#each selectedReview.criteria as criterion}<li>{safeDisplay(criterion.description)}</li>{/each}</ul><h4>Evidence references</h4><ul class="mono">{#each selectedReview.evidence_refs as ref}<li>{safeDisplay(ref)}</li>{/each}</ul><div class="fingerprint"><span>Evidence fingerprint</span><code>{selectedReview.fingerprint}</code></div><p class="boundary-note">This judgment verifies the recorded candidate only. It does not approve any consequential effect.</p><label>Type <code>APPROVE {selectedReview.fingerprint.slice(0,12)}</code>, <code>REJECT {selectedReview.fingerprint.slice(0,12)}</code>, or <code>REVISE {selectedReview.fingerprint.slice(0,12)}</code><input bind:value={reviewPhrase} autocomplete="off" /></label>{#if reviewPhrase.startsWith('REVISE')}<label>Revision feedback<textarea bind:value={revisionFeedback} required></textarea></label>{/if}<div class="actions three"><button class="danger" onclick={() => decideReview('REJECT')} disabled={busy}>Reject</button><button onclick={() => decideReview('REVISE')} disabled={busy}>Request revision</button><button class="primary" onclick={() => decideReview('APPROVE')} disabled={busy}>Approve evidence</button></div>{:else}<div class="empty">Select a review to compare the candidate result with its exact completion contract.</div>{/if}</div>
      </section>
    {:else}
      <section class="grid two"><div class="panel"><p class="eyebrow">Local boundary</p><h2>Dashboard session</h2><dl><div><dt>Organization</dt><dd>{safeDisplay(identity?.organization ?? 'Unavailable')}</dd></div><div><dt>Install mode</dt><dd>{safeDisplay(identity?.mode ?? 'Unavailable')}</dd></div><div><dt>Agent OS</dt><dd>{safeDisplay(identity?.version ?? 'Unavailable')}</dd></div><div><dt>Expires</dt><dd>{safeDisplay(identity?.session_expires_at ?? 'Unavailable')}</dd></div></dl></div><div class="panel"><p class="eyebrow">Diagnostics</p><h2>Read-only system checks</h2><p>Use <code>agentos doctor</code> for configuration, credential, service, private-gateway, and SQLite integrity checks.</p><pre>agentos doctor
sudo agentos doctor</pre></div></section>
    {/if}
  </main>
</div>
