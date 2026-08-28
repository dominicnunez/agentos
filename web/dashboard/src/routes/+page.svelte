<script lang="ts">
  import { onMount } from 'svelte';
  import { APIError, api, connect, emptyJSONPost, identifier, verifiedDownload } from '$lib/api';
  import { buildEvidenceBundle } from '$lib/archive';
  import { DisplayError } from '$lib/display-error';
  import { approvalRetryBinding, completionReviewFeedback, confirmationMessageID, confirmationRetryBinding, discardConfirmationRetry, discardStrategyRetry, loadAllCompletionReviews, matchesConfirmationRetry, matchesStrategyRetry, parseApprovalRetryBinding, parseConfirmationRetryBinding, parseReviewRetryBinding, parseStrategyRetryBinding, replayApprovalDecision, replayCompletionReviewDecision, reviewRetryBinding, safeDisplay, sameCompletionContract, snapshotCompletionEvidence, strategyRetryBinding, terminalApproval, terminalCompletionReview, validateArtifactSelections, validateCompletionFields } from '$lib/governance';
  import type { ApprovalRetryBinding, ReviewRetryBinding, StrategyRetryBinding } from '$lib/governance';
  import { formatDisplayMessage, resolveDisplayLocale } from '$lib/i18n';
  import type { DisplayLocale, DisplayMessageID, DisplayMessageValues } from '$lib/i18n';
  import '$lib/app.css';
  import type { Approval, CompletionReview, CompletionReviewPage, DashboardIdentity, GovernanceFinding, GovernanceInspection, IntentDraft, OrganizationSnapshot, TaskView } from '$lib/types';

  type Section = 'overview' | 'organization' | 'work' | 'approvals' | 'reviews' | 'governance' | 'system';
  type IntentList = 'context' | 'deliverables' | 'completion_criteria' | 'constraints';
  type GoalSummary = OrganizationSnapshot['goals'][number];
  type WorkSummary = OrganizationSnapshot['works'][number];
  type TaskSummary = OrganizationSnapshot['tasks'][number];
  type OrganizationIndex = {
    goalsByMission: Map<string, GoalSummary[]>;
    worksByGoal: Map<string, WorkSummary[]>;
    tasksByWork: Map<string, TaskSummary[]>;
    unalignedWorks: WorkSummary[];
    activeWorkCount: number;
  };
  const pendingConfirmationKey = 'agentos.dashboard.pending-confirmation';
  const pendingApprovalKey = 'agentos.dashboard.pending-approval';
  const pendingReviewKey = 'agentos.dashboard.pending-review';
  const pendingStrategyStorageKey = 'agentos.dashboard.pending-strategy';
  const navigation: readonly [Section, DisplayMessageID][] = [
    ['overview', 'navigation.overview'],
    ['organization', 'navigation.organization'],
    ['work', 'navigation.work'],
    ['approvals', 'navigation.approvals'],
    ['reviews', 'navigation.reviews'],
    ['governance', 'navigation.governance'],
    ['system', 'navigation.system']
  ];
  const sectionTitles: Readonly<Record<Section, DisplayMessageID>> = {
    overview: 'section.overview',
    organization: 'section.organization',
    work: 'section.work',
    approvals: 'section.approvals',
    reviews: 'section.reviews',
    governance: 'section.governance',
    system: 'section.system'
  };
  const intentGroups: readonly (readonly [DisplayMessageID, IntentList])[] = [
    ['work.context', 'context'],
    ['work.deliverables', 'deliverables'],
    ['work.doneWhen', 'completion_criteria'],
    ['work.requirements', 'constraints']
  ];

  let section: Section = 'overview';
  let displayLocale: DisplayLocale = 'en';
  let identity: DashboardIdentity | null = null;
  let organization: OrganizationSnapshot | null = null;
  let organizationIndex = emptyOrganizationIndex();
  let active: TaskView | null = null;
  let approvals: Approval[] = [];
  let reviews: CompletionReview[] = [];
  let selectedApproval: Approval | null = null;
  let selectedReview: CompletionReview | null = null;
  let governance: GovernanceInspection | null = null;
  let selectedFinding: GovernanceFinding | null = null;
  let task: TaskView | null = null;
  let taskID = '';
  let workText = '';
  let executionKind = '';
  let selectedGoalID = '';
  let missionStatement = '';
  let goalObjective = '';
  let goalMode: 'TARGET' | 'CONTINUOUS' = 'TARGET';
  let goalCriteria = '';
  let pendingStrategy: StrategyRetryBinding | null = null;
  let conversationID = '';
  let busy = false;
  let error = '';
  let notice = '';
  let approvalPhrase = '';
  let reviewPhrase = '';
  let reviewFeedback = '';
  let completionFields: Record<string, string> = {};
  let completionFiles: Record<string, File[]> = {};
  let pendingWorkMessageID = '';
  let pendingWorkKey = '';
  let pendingAbandonMessageID = '';
  let pendingAbandonConversationID = '';
  let completionMessageID = '';
  let taskInput = '';
  let pendingTaskInputMessageID = '';
  let pendingTaskInputKey = '';
  let pendingApprovalDecision: ApprovalRetryBinding | null = null;
  let pendingReviewDecision: ReviewRetryBinding | null = null;
  let refreshGeneration = 0;

  onMount(async () => {
    displayLocale = resolveDisplayLocale(navigator.languages);
    document.documentElement.lang = displayLocale;
    try {
      identity = await connect();
      const recoveryFailures: string[] = [];
      try {
        pendingApprovalDecision = parseApprovalRetryBinding(sessionStorage.getItem(pendingApprovalKey));
      } catch (cause) {
        sessionStorage.removeItem(pendingApprovalKey);
        recoveryFailures.push(message(cause));
      }
      try {
        pendingReviewDecision = parseReviewRetryBinding(sessionStorage.getItem(pendingReviewKey));
      } catch (cause) {
        sessionStorage.removeItem(pendingReviewKey);
        recoveryFailures.push(message(cause));
      }
      try {
        const recovered = await recoverPendingStrategy();
        if (recovered) {
          refreshGeneration += 1;
          setOrganization(recovered);
          notice = ui('notice.recoveredStrategy');
        }
      } catch (cause) {
        recoveryFailures.push(message(cause));
      }
      try {
        const recovered = await recoverPendingConfirmation();
        if (recovered) {
          task = recovered;
          taskID = recovered.task_id;
          notice = ui('notice.recoveredTask', { task: safeDisplay(recovered.task_id) });
        }
      } catch (cause) {
        recoveryFailures.push(message(cause));
      }
      if (!task) {
        try {
          task = await api<TaskView>('/api/v1/user/tasks/recent');
          taskID = task.task_id;
        } catch (cause) {
          if (!(cause instanceof APIError && cause.status === 404)) throw cause;
        }
      }
      await refresh();
      if (recoveryFailures.length) error = ui('error.recoveryFailed', { details: [...recoveryFailures, ...(error ? [error] : [])].join(' ') });
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
    const [organizationResult, activeResult, approvalResult, reviewResult, taskResult] = await Promise.allSettled([
      loadOrganization(),
      loadActiveIntent(),
      loadApprovals(),
      loadReviews(),
      displayedTask ? loadTask(displayedTask.task_id) : Promise.resolve(null)
    ]);
    if (generation !== refreshGeneration) return;
    const failures = [organizationResult, activeResult, approvalResult, reviewResult, taskResult]
      .filter((result) => result.status === 'rejected')
      .map((result) => message((result as PromiseRejectedResult).reason));
    if (organizationResult.status === 'fulfilled') {
      if (organizationResult.value) setOrganization(organizationResult.value);
      else {
        organization = null;
        organizationIndex = emptyOrganizationIndex();
      }
    }
    if (activeResult.status === 'fulfilled') {
      if (activeResult.value) {
        active = activeResult.value;
        conversationID = activeResult.value.conversation_id;
        selectedGoalID = activeResult.value.selected_goal_id ?? '';
      } else if (!hasStoredConfirmationRetry(displayedActive)) {
        active = null;
        conversationID = '';
        executionKind = '';
        selectedGoalID = '';
      }
    }
    if (approvalResult.status === 'fulfilled') {
      approvals = approvalResult.value;
      if (pendingApprovalDecision) {
        const recovered = approvals.find((item) => item.approval_id === pendingApprovalDecision?.approval_id);
        if (recovered && terminalApproval(recovered)) {
          const recorded = recovered.status === 'APPROVED' ? 'APPROVE' : 'DENY';
          if (recorded !== pendingApprovalDecision.decision) notice = ui('notice.otherApprovalDecision', { status: safeDisplay(recovered.status) });
          selectedApproval = recovered;
          clearPendingApprovalDecision();
        }
      }
    }
    if (reviewResult.status === 'fulfilled') {
      reviews = reviewResult.value;
      if (pendingReviewDecision) {
        const recovered = reviews.find((item) => item.review_id === pendingReviewDecision?.review_id);
        if (recovered && terminalCompletionReview(recovered)) {
          if (recovered.state !== pendingReviewDecision.decision || (recovered.state !== 'APPROVE' && recovered.feedback !== pendingReviewDecision.feedback)) notice = ui('notice.otherReviewDecision', { state: safeDisplay(recovered.state) });
          selectedReview = recovered;
          clearPendingReviewDecision();
        }
      }
    }
    if (taskResult.status === 'fulfilled' && taskResult.value) {
      if (!sameCompletionContract(displayedTask?.completion_contract, taskResult.value.completion_contract)) clearCompletionEvidence();
      task = taskResult.value;
      taskID = taskResult.value.task_id;
      if (taskResult.value.state !== 'INPUT_REQUIRED' || taskResult.value.task_id !== displayedTask?.task_id || taskResult.value.updated_at !== displayedTask.updated_at || taskResult.value.prompt !== displayedTask.prompt) clearTaskInput();
    }
    if (failures.length) error = ui('error.refreshFailed', { details: failures.join(' ') });
    selectedApproval = approvals.find((item) => item.approval_id === selectedApproval?.approval_id) ?? (selectedApproval && (terminalApproval(selectedApproval) || pendingApprovalDecision) ? selectedApproval : null);
    selectedReview = reviews.find((item) => item.review_id === selectedReview?.review_id) ?? (selectedReview && (terminalCompletionReview(selectedReview) || pendingReviewDecision) ? selectedReview : null);
  }

  async function downloadAIMSEvidence(): Promise<void> {
    busy = true;
    error = '';
    notice = '';
    try {
      const evidence = await verifiedDownload('/api/v1/user/aims/evidence');
      const bundle = buildEvidenceBundle(evidence.body, evidence.sha256);
      downloadBlob(new Blob([bundle], { type: 'application/x-tar' }), 'agentos-aims-evidence.tar');
      notice = ui('notice.evidenceDownloaded', { digest: evidence.sha256.slice(0, 12) });
    } catch (cause) {
      error = message(cause);
    } finally {
      busy = false;
    }
  }

  async function openSection(next: Section): Promise<void> {
    section = next;
    if (next === 'governance' && !governance) await refreshGovernance();
  }

  async function refreshGovernance(): Promise<void> {
    if (!identity) return;
    busy = true;
    error = '';
    try {
      const report = await api<GovernanceInspection>('/api/v1/user/governance/inspection');
      governance = report;
      selectedFinding = report.findings.find((item) => item.id === selectedFinding?.id) ?? report.findings[0] ?? null;
    } catch (cause) {
      error = message(cause);
    } finally {
      busy = false;
    }
  }

  function downloadBlob(blob: Blob, filename: string): void {
    const href = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = href;
    link.download = filename;
    link.rel = 'noopener';
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(href), 0);
  }

  async function loadOrganization(): Promise<OrganizationSnapshot | null> {
    try {
      return await api<OrganizationSnapshot>('/api/v1/user/organization');
    } catch (cause) {
      if (cause instanceof APIError && cause.status === 404) return null;
      throw cause;
    }
  }

  async function loadActiveIntent(): Promise<TaskView | null> {
    try {
      return await api<TaskView>('/api/v1/user/intents/active');
    } catch (cause) {
      if (cause instanceof APIError && cause.status === 404) return null;
      throw cause;
    }
  }

  async function loadApprovals(): Promise<Approval[]> {
    const pending = (await api<{ approvals: Approval[] }>('/api/v1/control/approvals')).approvals;
    const recent = (await api<{ approvals: Approval[] }>('/api/v1/control/approvals/recent?limit=20')).approvals;
    const available = [...pending, ...recent.filter((item) => !pending.some((pendingItem) => pendingItem.approval_id === item.approval_id))];
    if (!pendingApprovalDecision) return available;
    let exact: Approval;
    try {
      exact = await api<Approval>(`/api/v1/control/approvals/${encodeURIComponent(pendingApprovalDecision.approval_id)}`);
    } catch (cause) {
      if (cause instanceof APIError && cause.status === 410) {
        clearPendingApprovalDecision();
        notice = ui('notice.approvalExpired');
        return available;
      }
      throw cause;
    }
    if (exact.approval_id !== pendingApprovalDecision.approval_id || exact.effect_fingerprint !== pendingApprovalDecision.fingerprint) throw new Error(ui('error.approvalChanged'));
    if (terminalApproval(exact)) {
      return [...available.filter((item) => item.approval_id !== exact.approval_id), exact];
    } else {
      exact = await replayApprovalDecision(api, exact, pendingApprovalDecision.decision);
    }
    return [...available.filter((item) => item.approval_id !== exact.approval_id), exact];
  }

  async function loadReviews(): Promise<CompletionReview[]> {
    const pending = await loadAllCompletionReviews((after) => api<CompletionReviewPage>(`/api/v1/user/reviews?limit=100${after ? `&after=${encodeURIComponent(after)}` : ''}`));
    const recent = (await api<CompletionReviewPage>('/api/v1/user/reviews/recent?limit=20')).reviews;
    const available = [...pending, ...recent.filter((item) => !pending.some((pendingItem) => pendingItem.review_id === item.review_id))];
    if (!pendingReviewDecision) return available;
    let exact = await api<CompletionReview>(`/api/v1/user/reviews/${encodeURIComponent(pendingReviewDecision.task_id)}/records/${encodeURIComponent(pendingReviewDecision.review_id)}`);
    if (exact.review_id !== pendingReviewDecision.review_id || exact.fingerprint !== pendingReviewDecision.fingerprint) {
      throw new Error(ui('error.reviewChanged'));
    }
    if (terminalCompletionReview(exact)) {
      return [...available.filter((item) => item.review_id !== exact.review_id), exact];
    }
    exact = await replayCompletionReviewDecision(api, exact, pendingReviewDecision);
    return [...available.filter((item) => item.review_id !== exact.review_id), exact];
  }

  async function loadTask(id: string): Promise<TaskView> {
    const current = await api<TaskView>(`/api/v1/user/tasks/${encodeURIComponent(id)}`);
    let recoveryPath = '';
    if (current.completion_recovery_required) recoveryPath = 'completion';
    if (current.input_recovery_required) recoveryPath = 'input';
    if (!recoveryPath) return current;
    try {
      return await api<TaskView>(`/api/v1/user/tasks/${encodeURIComponent(id)}/${recoveryPath}/recover`, emptyJSONPost());
    } catch (cause) {
      if (cause instanceof APIError && cause.status === 404) return current;
      throw cause;
    }
  }

  async function submitWork(): Promise<void> {
    const text = workText;
    if (!text.trim()) return;
    if (!conversationID) conversationID = identifier('user');
    const currentConversation = conversationID;
    const requestKey = JSON.stringify([currentConversation, text, executionKind, selectedGoalID]);
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
          ...(executionKind ? { execution_kind: executionKind } : {}),
          ...(selectedGoalID ? { goal_id: selectedGoalID } : {})
        })
      });
      pendingWorkMessageID = '';
      pendingWorkKey = '';
      if (workText === text) workText = '';
      notice = active.prompt || ui('notice.proposalUpdated');
    });
  }

  async function confirmIntent(): Promise<void> {
    if (!active?.intent || !active.conversation_id) return;
    const draft = active.intent;
    const currentConversation = active.conversation_id;
    await action(async () => {
      const binding = confirmationRetryBinding(currentConversation, draft.fingerprint);
      sessionStorage.setItem(pendingConfirmationKey, JSON.stringify(binding));
      let confirmed: TaskView;
      try {
        confirmed = await sendConfirmation(binding.conversation_id, binding.fingerprint);
      } catch (cause) {
        clearTerminalConfirmationRetry(cause);
        throw cause;
      }
      sessionStorage.removeItem(pendingConfirmationKey);
      clearCompletionEvidence();
      executionKind = '';
      selectedGoalID = '';
      pendingWorkMessageID = '';
      pendingWorkKey = '';
      active = null;
      task = confirmed;
      taskID = confirmed.task_id;
      clearTaskInput();
      conversationID = '';
      notice = ui('notice.taskCreated', { task: safeDisplay(confirmed.task_id), work: safeDisplay(confirmed.work_id || '') });
      await refresh();
    });
  }

  async function abandonIntent(): Promise<void> {
    if (!active?.conversation_id) return;
    const currentConversation = active.conversation_id;
    if (!pendingAbandonMessageID || pendingAbandonConversationID !== currentConversation) {
      pendingAbandonMessageID = identifier('abandon');
      pendingAbandonConversationID = currentConversation;
    }
    await action(async () => {
      await api<TaskView>(`/api/v1/user/intents/${encodeURIComponent(currentConversation)}/abandon`, {
        method: 'POST',
        body: JSON.stringify({ message_id: pendingAbandonMessageID })
      });
      active = null;
      conversationID = '';
      selectedGoalID = '';
      executionKind = '';
      pendingWorkMessageID = '';
      pendingWorkKey = '';
      pendingAbandonMessageID = '';
      pendingAbandonConversationID = '';
      notice = ui('notice.intakeAbandoned');
      await refresh();
    });
  }

  function selectableGoals(): GoalSummary[] {
    if (!organization) return [];
    const activeMissions = new Set(organization.missions.filter((mission) => mission.status === 'ACTIVE').map((mission) => mission.id));
    return organization.goals.filter((goal) => goal.status === 'ACTIVE' && activeMissions.has(goal.mission_id));
  }

  function strategyCriteria(): string[] {
    return goalCriteria.split(/\r?\n/).map((criterion) => criterion.trim()).filter(Boolean);
  }

  function canBootstrapStrategy(): boolean {
    return Boolean(identity && canCreateInitialStrategy() && missionStatement.trim() && goalObjective.trim() && strategyCriteria().length);
  }

  function canCreateInitialStrategy(): boolean {
    return !organization || (organization.missions.length === 0 && organization.goals.length === 0);
  }

  async function bootstrapStrategy(): Promise<void> {
    const statement = missionStatement.trim();
    const objective = goalObjective.trim();
    const criteria = strategyCriteria();
    if (!statement || !objective || !criteria.length) return;
    if (pendingStrategy && !matchesStrategyRetry(pendingStrategy, statement, objective, goalMode, criteria)) {
      error = ui('error.strategyRecovery');
      return;
    }
    await action(async () => {
      const binding = pendingStrategy ?? strategyRetryBinding(
        identifier('strategy'), identifier('mission'), statement, identifier('goal'), objective, goalMode, criteria
      );
      pendingStrategy = binding;
      sessionStorage.setItem(pendingStrategyStorageKey, JSON.stringify(binding));
      let updated: OrganizationSnapshot;
      try {
        updated = await sendStrategy(binding);
      } catch (cause) {
        if (cause instanceof APIError && discardStrategyRetry(cause.status)) clearPendingStrategy();
        throw cause;
      }
      refreshGeneration += 1;
      setOrganization(updated);
      missionStatement = '';
      goalObjective = '';
      goalMode = 'TARGET';
      goalCriteria = '';
      clearPendingStrategy();
      notice = ui('notice.strategyDurable');
    });
  }

  function sendStrategy(binding: StrategyRetryBinding): Promise<OrganizationSnapshot> {
    return api<OrganizationSnapshot>('/api/v1/user/strategy/bootstrap', {
      method: 'POST',
      body: JSON.stringify(binding)
    });
  }

  async function recoverPendingStrategy(): Promise<OrganizationSnapshot | null> {
    let binding: StrategyRetryBinding | null;
    try {
      binding = parseStrategyRetryBinding(sessionStorage.getItem(pendingStrategyStorageKey));
    } catch (cause) {
      sessionStorage.removeItem(pendingStrategyStorageKey);
      pendingStrategy = null;
      throw cause;
    }
    if (!binding) return null;
    pendingStrategy = binding;
    missionStatement = binding.mission_statement;
    goalObjective = binding.goal_objective;
    goalMode = binding.goal_mode;
    goalCriteria = binding.success_criteria.join('\n');
    try {
      const updated = await sendStrategy(binding);
      clearPendingStrategy();
      missionStatement = '';
      goalObjective = '';
      goalMode = 'TARGET';
      goalCriteria = '';
      return updated;
    } catch (cause) {
      if (cause instanceof APIError && discardStrategyRetry(cause.status)) clearPendingStrategy();
      throw cause;
    }
  }

  function clearPendingStrategy(): void {
    pendingStrategy = null;
    sessionStorage.removeItem(pendingStrategyStorageKey);
  }

  async function recoverPendingConfirmation(): Promise<TaskView | null> {
    let binding;
    try {
      binding = parseConfirmationRetryBinding(sessionStorage.getItem(pendingConfirmationKey));
    } catch (cause) {
      sessionStorage.removeItem(pendingConfirmationKey);
      throw cause;
    }
    if (!binding) return null;
    try {
      const recovered = await sendConfirmation(binding.conversation_id, binding.fingerprint);
      sessionStorage.removeItem(pendingConfirmationKey);
      return recovered;
    } catch (cause) {
      clearTerminalConfirmationRetry(cause);
      throw cause;
    }
  }

  function hasStoredConfirmationRetry(view: TaskView | null): boolean {
    try {
      return matchesConfirmationRetry(view, parseConfirmationRetryBinding(sessionStorage.getItem(pendingConfirmationKey)));
    } catch {
      sessionStorage.removeItem(pendingConfirmationKey);
      return false;
    }
  }

  function clearTerminalConfirmationRetry(cause: unknown): void {
    if (cause instanceof APIError && discardConfirmationRetry(cause.status)) sessionStorage.removeItem(pendingConfirmationKey);
  }

  function sendConfirmation(conversation: string, fingerprint: string): Promise<TaskView> {
    return api<TaskView>(`/api/v1/user/intents/${encodeURIComponent(conversation)}/confirm`, {
      method: 'POST',
      body: JSON.stringify({ message_id: confirmationMessageID(fingerprint), fingerprint })
    });
  }

  async function findTask(): Promise<void> {
    const id = taskID.trim();
    if (!id) return;
    await action(async () => {
      task = await loadTask(id);
      clearCompletionEvidence();
      clearTaskInput();
    });
  }

  async function submitTaskInput(): Promise<void> {
    if (!task?.conversation_id || task.state !== 'INPUT_REQUIRED' || task.completion_contract) return;
    const text = taskInput;
    if (!text.trim()) return;
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
      if (taskInput === text) taskInput = '';
      notice = task.prompt || ui('notice.inputRecorded');
      await refresh();
    });
  }

  async function submitCompletion(): Promise<void> {
    if (!task?.completion_contract || task.review_required) return;
    const currentTask = task;
    const evidence = snapshotCompletionEvidence(completionFields, completionFiles);
    if (!completionMessageID) completionMessageID = identifier('completion');
    const messageID = completionMessageID;
    await action(async () => {
      const fieldError = validateCompletionFields(currentTask.completion_contract!.required_fields ?? [], evidence.fields);
      if (fieldError) throw fieldError;
      const selectionError = validateArtifactSelections(currentTask.completion_contract!.artifact_requirements ?? [], evidence.files);
      if (selectionError) throw selectionError;
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
      notice = ui('notice.completionSubmitted');
      await refresh();
    });
  }

  async function decideApproval(decision: 'APPROVE' | 'DENY'): Promise<void> {
    if (!selectedApproval) return;
    if (pendingApprovalDecision) {
      error = ui('error.approvalRecovery');
      return;
    }
    const approval = selectedApproval;
    if (terminalApproval(approval)) return;
    const expected = decision === 'APPROVE' ? `APPROVE ${approval.effect_fingerprint.slice(0, 12)}` : 'DENY';
    if (approvalPhrase.trim() !== expected) {
      error = ui('error.typeExact', { expected });
      return;
    }
    pendingApprovalDecision = approvalRetryBinding(approval.approval_id, approval.effect_fingerprint, decision);
    sessionStorage.setItem(pendingApprovalKey, JSON.stringify(pendingApprovalDecision));
    await action(async () => {
      let current = await api<Approval>(`/api/v1/control/approvals/${encodeURIComponent(approval.approval_id)}`);
      if (current.effect_fingerprint !== approval.effect_fingerprint) throw new Error(ui('error.approvalFingerprintChanged'));
      if (terminalApproval(current)) {
        const recorded = current.status === 'APPROVED' ? 'APPROVE' : 'DENY';
        if (recorded !== decision) throw new Error(ui('error.effectAlready', { status: safeDisplay(current.status.toLowerCase()) }));
        selectedApproval = current;
        clearPendingApprovalDecision();
        approvalPhrase = '';
        notice = ui(decision === 'APPROVE' ? 'notice.effectApproved' : 'notice.effectDenied');
        await refresh();
        return;
      }
      selectedApproval = await replayApprovalDecision(api, current, decision);
      clearPendingApprovalDecision();
      approvalPhrase = '';
      notice = ui(decision === 'APPROVE' ? 'notice.effectApproved' : 'notice.effectDenied');
      await refresh();
    });
  }

  async function decideReview(decision: 'APPROVE' | 'REJECT' | 'REVISE'): Promise<void> {
    if (!selectedReview) return;
    if (pendingReviewDecision) {
      error = ui('error.reviewRecovery');
      return;
    }
    const review = selectedReview;
    if (terminalCompletionReview(review)) return;
    const expected = `${decision} ${review.fingerprint.slice(0, 12)}`;
    if (reviewPhrase.trim() !== expected) {
      error = ui('error.typeExact', { expected });
      return;
    }
    if (decision === 'REVISE' && !reviewFeedback.trim()) {
      error = ui('error.revisionFeedback');
      return;
    }
    const feedback = completionReviewFeedback(decision, reviewFeedback);
    pendingReviewDecision = reviewRetryBinding(review.task_id, review.review_id, review.fingerprint, decision, feedback);
    sessionStorage.setItem(pendingReviewKey, JSON.stringify(pendingReviewDecision));
    await action(async () => {
      const current = await api<CompletionReview>(`/api/v1/user/reviews/${encodeURIComponent(review.task_id)}/records/${encodeURIComponent(review.review_id)}`);
      if (current.review_id !== review.review_id || current.fingerprint !== review.fingerprint) throw new Error(ui('error.reviewChanged'));
      if (terminalCompletionReview(current)) {
        if (current.state !== decision || (decision !== 'APPROVE' && current.feedback !== feedback)) throw new Error(ui('error.reviewDecisionChanged'));
        selectedReview = current;
        clearPendingReviewDecision();
        reviewPhrase = '';
        reviewFeedback = '';
        notice = ui('notice.reviewAlreadyMarked', { decision });
        await refresh();
        return;
      }
      selectedReview = await replayCompletionReviewDecision(api, current, pendingReviewDecision!);
      clearPendingReviewDecision();
      reviewPhrase = '';
      reviewFeedback = '';
      notice = ui('notice.reviewMarked', { decision });
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

  function clearPendingApprovalDecision(): void {
    pendingApprovalDecision = null;
    sessionStorage.removeItem(pendingApprovalKey);
  }

  function clearPendingReviewDecision(): void {
    pendingReviewDecision = null;
    sessionStorage.removeItem(pendingReviewKey);
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
    if (cause instanceof DisplayError) return ui(cause.messageID, cause.values);
    return cause instanceof Error ? safeDisplay(cause.message) : ui('error.requestFailed');
  }

  function values(draft: IntentDraft, key: IntentList) {
    return draft[key] ?? [];
  }

  function pendingApprovalCount(): number {
    return approvals.filter((item) => !terminalApproval(item)).length;
  }

  function pendingReviewCount(): number {
    return reviews.filter((item) => !terminalCompletionReview(item)).length;
  }

  function emptyOrganizationIndex(): OrganizationIndex {
    return { goalsByMission: new Map(), worksByGoal: new Map(), tasksByWork: new Map(), unalignedWorks: [], activeWorkCount: 0 };
  }

  function setOrganization(next: OrganizationSnapshot): void {
    const index = emptyOrganizationIndex();
    for (const goal of next.goals) addIndexed(index.goalsByMission, goal.mission_id, goal);
    for (const work of next.works) {
      if (work.goal_id) addIndexed(index.worksByGoal, work.goal_id, work);
      else index.unalignedWorks.push(work);
      if (work.status === 'ACTIVE') index.activeWorkCount++;
    }
    for (const item of next.tasks) addIndexed(index.tasksByWork, item.work_id, item);
    organizationIndex = index;
    organization = next;
  }

  function addIndexed<T>(index: Map<string, T[]>, key: string, value: T): void {
    const values = index.get(key);
    if (values) values.push(value);
    else index.set(key, [value]);
  }

  function ui(id: DisplayMessageID, values?: DisplayMessageValues): string {
    return formatDisplayMessage(displayLocale, id, values);
  }
</script>

<svelte:head>
  <title>{ui('app.title')}</title>
  <meta name="description" content={ui('app.description')} />
</svelte:head>

<div class="shell">
  <aside>
    <div class="brand"><span class="mark">AO</span><div><strong>{ui('app.title')}</strong><small>{ui('app.brandSubtitle')}</small></div></div>
    <nav aria-label={ui('navigation.label')}>
      {#each navigation as item}
        <button class:active={section === item[0]} onclick={() => openSection(item[0])}><span class="nav-dot"></span>{ui(item[1])}</button>
      {/each}
    </nav>
    <div class="identity"><span class:online={Boolean(identity)}></span><div><strong>{safeDisplay(identity?.organization ?? ui('identity.notConnected'))}</strong><small>{identity ? ui('identity.installation', { mode: safeDisplay(identity.mode) }) : ui('identity.sessionRequired')}</small></div></div>
  </aside>

  <main>
    <header><div><p class="eyebrow">{ui('header.organization')}</p><h1>{ui(sectionTitles[section])}</h1></div><button class="quiet" onclick={() => section === 'governance' ? refreshGovernance() : refresh()} disabled={!identity || busy}>{ui('header.refresh')}</button></header>
    {#if error}<div class="banner error" role="alert">{safeDisplay(error)}</div>{/if}
    {#if notice}<div class="banner notice" role="status">{safeDisplay(notice)}</div>{/if}

    {#if section === 'overview'}
      <section class="metrics">
        <button onclick={() => section='organization'}><span>{ui('overview.activeWork')}</span><strong>{organizationIndex.activeWorkCount}</strong><small>{organization ? ui('overview.organizationCounts', { missions: organization.missions.length, goals: organization.goals.length, teams: organization.teams.length, agents: organization.agents.length }) : ui('overview.organizationUnavailable')}</small></button>
        <button onclick={() => section='approvals'}><span>{ui('overview.approvals')}</span><strong>{pendingApprovalCount()}</strong><small>{ui('overview.effectsAwaiting')}</small></button>
        <button onclick={() => section='reviews'}><span>{ui('overview.completionReviews')}</span><strong>{pendingReviewCount()}</strong><small>{ui('overview.evidenceAwaiting')}</small></button>
      </section>
      <section class="panel mission">
        <div><p class="eyebrow">{ui('overview.currentOrganization')}</p><h2>{safeDisplay(organization?.organization.name ?? identity?.organization ?? ui('overview.connect'))}</h2><p>{ui('overview.organizationSummary')}</p><button class="text" onclick={() => section='organization'}>{ui('overview.openOrganization')}</button></div>
        <div class="flow"><span>{ui('overview.mission')}</span><i></i><span>{ui('overview.goal')}</span><i></i><span>{ui('overview.work')}</span><i></i><span>{ui('overview.task')}</span></div>
      </section>
      <section class="grid two">
        <div class="panel"><div class="panel-title"><h2>{ui('overview.continueWork')}</h2><button class="text" onclick={() => section='work'}>{ui('overview.openWork')}</button></div>{#if active}<span class="status">{safeDisplay(active.state)}</span><h3>{safeDisplay(active.intent?.objective ?? active.prompt ?? ui('overview.intakeConversation'))}</h3><p class="mono">{safeDisplay(active.conversation_id)}</p>{:else}<div class="empty">{ui('overview.noIntake')}</div>{/if}</div>
        <div class="panel"><div class="panel-title"><h2>{ui('overview.governanceQueue')}</h2></div><div class="queue"><div><strong>{pendingApprovalCount()}</strong><span>{ui('overview.effectDecisions')}</span></div><div><strong>{pendingReviewCount()}</strong><span>{ui('overview.evidenceReviews')}</span></div></div></div>
      </section>
    {:else if section === 'organization'}
      {#if canCreateInitialStrategy()}
        <section class="panel strategy-setup">
          <div><p class="eyebrow">{ui('organization.setupEyebrow')}</p><h2>{ui('organization.setupTitle')}</h2><p>{ui('organization.setupSummary')}</p></div>
          <div class="strategy-fields">
            <label>{ui('organization.missionLabel')}<textarea bind:value={missionStatement} disabled={busy} rows="3" placeholder={ui('organization.missionPlaceholder')}></textarea></label>
            <label>{ui('organization.goalLabel')}<input bind:value={goalObjective} disabled={busy} placeholder={ui('organization.goalPlaceholder')} /></label>
            <label>{ui('organization.modeLabel')}<select bind:value={goalMode} disabled={busy}><option value="TARGET">{ui('organization.target')}</option><option value="CONTINUOUS">{ui('organization.continuous')}</option></select></label>
            <label>{ui('organization.successCriteria')}<textarea bind:value={goalCriteria} disabled={busy} rows="4" placeholder={ui('organization.successCriteriaPlaceholder')}></textarea><small>{ui('organization.successCriteriaHelp')}</small></label>
            <button class="primary" onclick={bootstrapStrategy} disabled={busy || !canBootstrapStrategy()}>{ui('organization.createStrategy')}</button>
            <p class="boundary-note">{ui('organization.strategyBoundary')}</p>
          </div>
        </section>
      {/if}
      {#if organization}
        <section class="organization-layout">
          <div class="panel organization-tree">
            <div class="panel-title"><div><p class="eyebrow">{ui('organization.durableDirection')}</p><h2>{safeDisplay(organization.organization.name)}</h2></div><span class="status">{ui('organization.policy', { version: safeDisplay(organization.organization.policy_version) })}</span></div>
            <p class="boundary-note">{ui('organization.projectionBoundary')}</p>
            {#if organization.missions.length}
              {#each organization.missions as mission}
                {@const missionGoals = organizationIndex.goalsByMission.get(mission.id) ?? []}
                <article class="mission-card">
                  <div class="organization-heading"><div><p class="eyebrow">{ui('organization.missionLabel')}</p><h3>{safeDisplay(mission.statement)}</h3><small class="mono">{ui('organization.revision', { id: safeDisplay(mission.id), version: mission.version })}</small></div><span class="status">{safeDisplay(mission.status)}</span></div>
                  {#if missionGoals.length}
                    {#each missionGoals as goal}
                      {@const goalWorks = organizationIndex.worksByGoal.get(goal.id) ?? []}
                      <div class="goal-card">
                        <div class="organization-heading"><div><p class="eyebrow">{ui('organization.goalEyebrow', { mode: safeDisplay(goal.mode) })}</p><h3>{safeDisplay(goal.objective)}</h3><small class="mono">{ui('organization.revision', { id: safeDisplay(goal.id), version: goal.version })}</small></div><span class="status">{safeDisplay(goal.status)}</span></div>
                        {#if goal.success_criteria.length}<h4>{ui('organization.successCriteria')}</h4><ul>{#each goal.success_criteria as criterion}<li>{safeDisplay(criterion)}</li>{/each}</ul>{/if}
                        {#if goalWorks.length}
                          {#each goalWorks as work}
                            {@const workTasks = organizationIndex.tasksByWork.get(work.id) ?? []}
                            <div class="work-card"><div class="organization-heading"><div><strong>{safeDisplay(work.objective)}</strong><small class="mono">{safeDisplay(work.id)}</small>{#if work.replaces_work_id}<small>{ui('organization.replaces', { id: safeDisplay(work.replaces_work_id) })}</small>{/if}</div><span class="status">{safeDisplay(work.status)}</span></div>
                              <small>{safeDisplay(work.mode)}{work.experiment_status ? ` · ${safeDisplay(work.experiment_status)}` : ''}</small>{#if work.trust_label}<span class="risk">{safeDisplay(work.trust_label)}</span>{/if}
                              {#if workTasks.length}<div class="task-dag">{#each workTasks as item}<div><span class="task-node"></span><p><strong>{safeDisplay(item.description)}</strong><small>{safeDisplay(item.execution_kind)} · {safeDisplay(item.model_inference_policy)} · {safeDisplay(item.status)}</small>{#if item.assignee_id}<small>{ui('organization.assignedTo', { type: safeDisplay(item.assignee_type ?? 'UNKNOWN'), id: safeDisplay(item.assignee_id) })}</small>{/if}{#if item.parent_id}<small>{ui('organization.parent', { id: safeDisplay(item.parent_id) })}</small>{/if}{#if item.depends_on.length}<small>{ui('organization.dependsOn', { ids: item.depends_on.map(safeDisplay).join(', ') })}</small>{/if}</p></div>{/each}</div>{/if}
                            </div>
                          {/each}
                        {:else}<div class="organization-empty">{ui('organization.noWorkForGoal')}</div>{/if}
                      </div>
                    {/each}
                  {:else}<div class="organization-empty">{ui('organization.noGoalsForMission')}</div>{/if}
                </article>
              {/each}
            {:else}<div class="organization-empty">{ui('organization.noMission')}</div>{/if}
            {#if organizationIndex.unalignedWorks.length}<article class="mission-card"><p class="eyebrow">{ui('organization.workWithoutGoal')}</p><h3>{ui('organization.unalignedWork')}</h3>{#each organizationIndex.unalignedWorks as work}{@const workTasks = organizationIndex.tasksByWork.get(work.id) ?? []}<div class="work-card"><div class="organization-heading"><div><strong>{safeDisplay(work.objective)}</strong><small class="mono">{safeDisplay(work.id)}</small>{#if work.replaces_work_id}<small>{ui('organization.replaces', { id: safeDisplay(work.replaces_work_id) })}</small>{/if}</div><span class="status">{safeDisplay(work.status)}</span></div><small>{safeDisplay(work.mode)}{work.experiment_status ? ` · ${safeDisplay(work.experiment_status)}` : ''}</small>{#if work.trust_label}<span class="risk">{safeDisplay(work.trust_label)}</span>{/if}{#if workTasks.length}<div class="task-dag">{#each workTasks as item}<div><span class="task-node"></span><p><strong>{safeDisplay(item.description)}</strong><small>{safeDisplay(item.execution_kind)} · {safeDisplay(item.model_inference_policy)} · {safeDisplay(item.status)}</small>{#if item.assignee_id}<small>{ui('organization.assignedTo', { type: safeDisplay(item.assignee_type ?? 'UNKNOWN'), id: safeDisplay(item.assignee_id) })}</small>{/if}{#if item.parent_id}<small>{ui('organization.parent', { id: safeDisplay(item.parent_id) })}</small>{/if}{#if item.depends_on.length}<small>{ui('organization.dependsOn', { ids: item.depends_on.map(safeDisplay).join(', ') })}</small>{/if}</p></div>{/each}</div>{/if}</div>{/each}</article>{/if}
          </div>
          <div class="organization-roster">
            <div class="panel agent-roster"><div class="panel-title"><div><p class="eyebrow">{ui('organization.durableStructure')}</p><h2>{ui('organization.teams')}</h2></div><span class="count">{organization.teams.length}</span></div>{#if organization.teams.length}{#each organization.teams as team}<article><div class="organization-heading"><div><h3>{safeDisplay(team.name)}</h3><small class="mono">{ui('organization.revision', { id: safeDisplay(team.id), version: team.version })}</small></div><span class="status">{safeDisplay(team.status)}</span></div>{#if team.mission}<p>{safeDisplay(team.mission)}</p>{/if}<p><strong>{ui('organization.members')}</strong> {team.member_agent_ids.length ? team.member_agent_ids.map(safeDisplay).join(', ') : ui('organization.none')}</p></article>{/each}{:else}<div class="organization-empty">{ui('organization.noTeams')}</div>{/if}</div>
            <div class="panel agent-roster"><div class="panel-title"><div><p class="eyebrow">{ui('organization.durableIdentities')}</p><h2>{ui('organization.agents')}</h2></div><span class="count">{organization.agents.length}</span></div>{#if organization.agents.length}{#each organization.agents as agent}<article><div class="organization-heading"><div><h3>{safeDisplay(agent.role)}</h3><small class="mono">{ui('organization.revision', { id: safeDisplay(agent.id), version: agent.version })}</small></div><span class:offline={!agent.available} class="status">{agent.available ? 'AVAILABLE' : 'UNAVAILABLE'}</span></div><dl><div><dt>{ui('organization.agent')}</dt><dd>{safeDisplay(agent.status)}</dd></div><div><dt>{ui('organization.blueprint')}</dt><dd>{safeDisplay(agent.blueprint_status)}</dd></div><div><dt>{ui('organization.profile')}</dt><dd>{safeDisplay(agent.execution_profile_status)}</dd></div><div><dt>{ui('organization.model')}</dt><dd>{safeDisplay(agent.model_provider)} / {safeDisplay(agent.model)}</dd></div><div><dt>{ui('organization.runtime')}</dt><dd>{safeDisplay(agent.runtime_adapter)}</dd></div></dl></article>{/each}{:else}<div class="organization-empty">{ui('organization.noAgents')}</div>{/if}</div>
          </div>
        </section>
      {:else}<div class="panel empty">{ui('organization.unavailable')}</div>{/if}
    {:else if section === 'work'}
      <section class="grid work-grid">
        <div class="panel composer"><p class="eyebrow">{ui('work.intakeEyebrow')}</p><h2>{active ? ui('work.continueConversation') : ui('work.organizationOutcome')}</h2><textarea bind:value={workText} disabled={busy} rows="7" placeholder={ui('work.outcomePlaceholder')}></textarea><label>{ui('work.goal')}<select bind:value={selectedGoalID} disabled={busy || Boolean(active)}><option value="">{ui('work.adHoc')}</option>{#each selectableGoals() as goal}<option value={goal.id}>{safeDisplay(goal.objective)} · {safeDisplay(goal.id)}</option>{/each}</select><small>{ui('work.goalBoundary')}</small></label><label>{ui('work.execution')}<select bind:value={executionKind} disabled={busy || Boolean(active)}><option value="">{ui('work.automatic')}</option><option value="DETERMINISTIC">{ui('work.deterministicHandler')}</option><option value="HUMAN">{ui('work.userTask')}</option></select><small>{ui('work.executionHelp')}</small></label><div class="actions"><button class="primary" onclick={submitWork} disabled={busy || !identity || !workText.trim()}>{ui('work.send')}</button><small>{ui('work.proposalBoundary')}</small></div></div>
        <div class="panel intent"><div class="panel-title"><div><p class="eyebrow">{ui('work.intentContract')}</p><h2>{ui('work.reviewBeforeStart')}</h2></div>{#if active?.state}<span class="status">{safeDisplay(active.state)}</span>{/if}</div>
          {#if active?.intent}
            {#if active.prompt}<div class="banner notice governed-text" role="status"><strong>{active.state === 'INPUT_REQUIRED' ? ui('work.moreInformation') : ui('work.reviewGuidance')}</strong><br />{safeDisplay(active.prompt)}</div>{/if}
            <h3>{safeDisplay(active.intent.objective)}</h3>
            <dl><div><dt>{ui('work.mode')}</dt><dd>{safeDisplay(active.intent.mode)}</dd></div>{#if active.intent.requested_execution_kind}<div><dt>{ui('work.requestedExecution')}</dt><dd>{safeDisplay(active.intent.requested_execution_kind)}</dd></div>{/if}{#if active.intent.goal}<div><dt>{ui('work.goal')}</dt><dd>{safeDisplay(active.intent.goal.value)}</dd></div>{/if}{#if active.intent.replaces_work}<div><dt>{ui('work.replacesWork')}</dt><dd>{safeDisplay(active.intent.replaces_work.value)}</dd></div>{/if}</dl>
            {#each intentGroups as group}
              {#if values(active.intent, group[1]).length}<h4>{ui(group[0])}</h4><ul>{#each values(active.intent, group[1]) as value}<li>{safeDisplay(value.value)}</li>{/each}</ul>{/if}
            {/each}
            {#if active.intent.resolved_decisions?.length}<h4>{ui('work.resolvedDecisions')}</h4><ul>{#each active.intent.resolved_decisions as decision}<li><strong>{safeDisplay(decision.subject)}:</strong> {safeDisplay(decision.value)}</li>{/each}</ul>{/if}
            {#if active.intent.consequence_candidates?.length}<h4>{ui('work.taskBoundaries')}</h4><ul>{#each active.intent.consequence_candidates as boundary}<li>{safeDisplay(boundary)}</li>{/each}</ul>{/if}
            <div class="fingerprint"><span>{ui('work.intentVersion', { version: active.intent.version })}</span><code>{active.intent.fingerprint}</code></div>
            <button class="primary wide" onclick={confirmIntent} disabled={busy || active.state !== 'AWAITING_CONFIRMATION'}>{ui('work.confirmIntent')}</button>
            <p class="boundary-note">{ui('work.confirmBoundary')}</p>
          {:else if active}<div class="empty"><p>{safeDisplay(active.prompt || ui('work.moreInformationFallback'))}</p></div>{:else}<div class="empty">{ui('work.beginIntake')}</div>{/if}
          {#if active}<button class="danger wide" onclick={abandonIntent} disabled={busy}>{ui('work.abandonIntake')}</button><p class="boundary-note">{ui('work.abandonBoundary')}</p>{/if}
        </div>
      </section>
      <section class="panel task-lookup"><div><p class="eyebrow">{ui('task.durableStatus')}</p><h2>{ui('task.find')}</h2></div><div class="inline"><input bind:value={taskID} placeholder={ui('task.idPlaceholder')} /><button onclick={findTask} disabled={busy || !taskID.trim()}>{ui('task.open')}</button></div>
        {#if task}<div class="task"><div><span class="status">{safeDisplay(task.state)}</span><h3>{safeDisplay(task.task_id)}</h3>{#if task.work_id}<p class="mono">{ui('task.work', { id: safeDisplay(task.work_id) })}</p>{/if}{#if task.mode}<p>{ui('task.mode')} <strong>{safeDisplay(task.mode)}</strong></p>{/if}{#if task.trust_label}<p class="risk">{ui('task.trust', { label: safeDisplay(task.trust_label) })}</p>{/if}{#if task.prompt}<p class="boundary-note governed-text">{safeDisplay(task.prompt)}</p>{/if}{#if task.result}<p class="governed-text">{safeDisplay(task.result)}</p>{/if}</div>
          {#if task.completion_recovery_required}<div class="empty"><p>{ui('task.completionRecovery')}</p><button onclick={() => refresh()} disabled={busy}>{ui('task.retryRecovery')}</button></div>{:else if task.input_recovery_required}<div class="empty"><p>{ui('task.inputRecovery')}</p><button onclick={() => refresh()} disabled={busy}>{ui('task.retryRecovery')}</button></div>{:else if task.review_required}<div class="empty"><p>{ui('task.reviewRequired')}</p><button class="text" onclick={() => section='reviews'}>{ui('task.openReviews')}</button></div>{:else if task.completion_contract}{#key `${task.task_id}:${task.completion_contract.task_version}`}<form onsubmit={(event) => { event.preventDefault(); submitCompletion(); }}><h4>{ui('task.requiredEvidence')}</h4>{#each task.completion_contract.required_fields ?? [] as field}<label>{safeDisplay(field.name)}<small>{ui('task.fieldBounds', { description: safeDisplay(field.description), minimum: field.min_bytes, maximum: field.max_bytes })}</small><textarea required disabled={busy} value={completionFields[field.name] ?? ''} oninput={(event) => setCompletionField(field.name, event.currentTarget.value)}></textarea></label>{/each}{#each task.completion_contract.artifact_requirements ?? [] as requirement}<label>{safeDisplay(requirement.role)}<small>{ui('task.artifactBounds', { minimum: requirement.min_count, maximum: requirement.max_count, mediaTypes: requirement.media_types.map(safeDisplay).join(', ') })}</small><input type="file" disabled={busy} required={requirement.min_count > 0} multiple={requirement.max_count > 1} accept={requirement.media_types.join(',')} onchange={(event) => setFiles(requirement.role, event)} /></label>{/each}<button class="primary" type="submit" disabled={busy}>{ui('task.submitEvidence')}</button></form>{/key}{:else if task.state === 'INPUT_REQUIRED' && task.conversation_id && task.user_input_allowed}<form onsubmit={(event) => { event.preventDefault(); submitTaskInput(); }}><h4>{ui('task.provideInput')}</h4><label>{ui('task.response')}<textarea bind:value={taskInput} disabled={busy} required placeholder={ui('task.responsePlaceholder')}></textarea></label><button class="primary" type="submit" disabled={busy || !taskInput.trim()}>{ui('task.continue')}</button></form>{/if}
        </div>{/if}
      </section>
    {:else if section === 'approvals'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">{ui('approval.effectsEyebrow')}</p><h2>{ui('approval.title')}</h2></div><span class="count">{pendingApprovalCount()}</span></div>{#if approvals.length}{#each approvals as approval}<button class:selected={selectedApproval?.approval_id === approval.approval_id} onclick={() => {selectedApproval=approval; approvalPhrase='';}}><div><strong>{safeDisplay(approval.action)}</strong><span>{safeDisplay(approval.resource)}</span></div><div><span class="risk">{safeDisplay(approval.risk)}</span><small>{safeDisplay(approval.status)} · {safeDisplay(approval.boundary)}</small></div></button>{/each}{:else}<div class="empty">{ui('approval.empty')}</div>{/if}</div>
        <div class="panel detail">{#if selectedApproval}<p class="eyebrow">{ui('approval.effectEyebrow')}</p><h2>{safeDisplay(selectedApproval.canonical_effect_descriptor)}</h2><dl><div><dt>{ui('approval.status')}</dt><dd>{safeDisplay(selectedApproval.status)}</dd></div><div><dt>{ui('approval.action')}</dt><dd>{safeDisplay(selectedApproval.action)}</dd></div><div><dt>{ui('approval.resource')}</dt><dd>{safeDisplay(selectedApproval.resource)}</dd></div><div><dt>{ui('approval.scope')}</dt><dd>{safeDisplay(selectedApproval.scope)}</dd></div><div><dt>{ui('approval.boundary')}</dt><dd>{safeDisplay(selectedApproval.boundary)}</dd></div><div><dt>{ui('approval.risk')}</dt><dd>{safeDisplay(selectedApproval.risk)}</dd></div><div><dt>{ui('approval.urgency')}</dt><dd>{safeDisplay(selectedApproval.urgency)}</dd></div><div><dt>{ui('approval.expires')}</dt><dd>{safeDisplay(selectedApproval.expires_at ?? ui('approval.noExpiry'))}</dd></div><div><dt>{ui('approval.singleUse')}</dt><dd>{selectedApproval.single_use ? ui('approval.yes') : ui('approval.no')}</dd></div></dl>{#if Object.keys(selectedApproval.effect_arguments).length}<h4>{ui('approval.arguments')}</h4><pre>{safeDisplay(JSON.stringify(selectedApproval.effect_arguments, null, 2))}</pre>{/if}<div class="fingerprint"><span>{ui('approval.fingerprint')}</span><code>{selectedApproval.effect_fingerprint}</code></div>{#if selectedApproval.status === 'APPROVED' || selectedApproval.status === 'DENIED'}<p class="boundary-note">{ui('approval.recorded', { status: safeDisplay(selectedApproval.status) })}</p>{:else if pendingApprovalDecision}<p class="boundary-note">{ui('approval.recovering')}</p><button onclick={() => refresh()} disabled={busy}>{ui('approval.retry')}</button>{:else}<label>{ui('approval.type')} <code>APPROVE {selectedApproval.effect_fingerprint.slice(0,12)}</code> {ui('approval.or')} <code>DENY</code><input bind:value={approvalPhrase} autocomplete="off" /></label><div class="actions"><button class="danger" onclick={() => decideApproval('DENY')} disabled={busy}>{ui('approval.deny')}</button><button class="primary" onclick={() => decideApproval('APPROVE')} disabled={busy}>{ui('approval.approve')}</button></div>{/if}{:else}<div class="empty">{ui('approval.select')}</div>{/if}</div>
      </section>
    {:else if section === 'reviews'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">{ui('review.evidenceEyebrow')}</p><h2>{ui('review.title')}</h2></div><span class="count">{pendingReviewCount()}</span></div>{#if reviews.length}{#each reviews as review}<button class:selected={selectedReview?.review_id === review.review_id} onclick={() => {selectedReview=review; reviewPhrase=''; reviewFeedback='';}}><div><strong>{safeDisplay(review.objective)}</strong><span>{safeDisplay(review.task_id)}</span></div><span class="status">{safeDisplay(review.state)}</span></button>{/each}{:else}<div class="empty">{ui('review.empty')}</div>{/if}</div>
        <div class="panel detail">{#if selectedReview}<p class="eyebrow">{ui('review.candidateEyebrow')}</p><h2>{safeDisplay(selectedReview.objective)}</h2><blockquote>{safeDisplay(selectedReview.candidate_result ?? selectedReview.result ?? ui('review.noResult'))}</blockquote><h4>{ui('review.doneWhen')}</h4><ul>{#each selectedReview.criteria as criterion}<li>{safeDisplay(criterion.description)}</li>{/each}</ul><h4>{ui('review.evidenceReferences')}</h4><ul class="mono">{#each selectedReview.evidence_refs as ref}<li>{safeDisplay(ref)}</li>{/each}</ul><div class="fingerprint"><span>{ui('review.fingerprint')}</span><code>{selectedReview.fingerprint}</code></div><p class="boundary-note">{ui('review.boundary')}</p>{#if selectedReview.state !== 'PENDING'}<dl><div><dt>{ui('review.decision')}</dt><dd>{safeDisplay(selectedReview.state)}</dd></div><div><dt>{ui('review.reviewer')}</dt><dd>{safeDisplay(selectedReview.reviewer_id ?? ui('review.noReviewer'))}</dd></div></dl>{#if selectedReview.feedback}<h4>{ui('review.recordedFeedback')}</h4><pre>{safeDisplay(selectedReview.feedback)}</pre>{/if}<p class="boundary-note">{ui('review.immutable')}</p>{:else if pendingReviewDecision}<p class="boundary-note">{ui('review.recovering')}</p><button onclick={() => refresh()} disabled={busy}>{ui('approval.retry')}</button>{:else}<label>{ui('approval.type')} <code>APPROVE {selectedReview.fingerprint.slice(0,12)}</code>, <code>REJECT {selectedReview.fingerprint.slice(0,12)}</code>, {ui('approval.or')} <code>REVISE {selectedReview.fingerprint.slice(0,12)}</code><input bind:value={reviewPhrase} autocomplete="off" /></label>{#if reviewPhrase.startsWith('REJECT') || reviewPhrase.startsWith('REVISE')}<label>{ui('review.feedback')}<textarea bind:value={reviewFeedback} required={reviewPhrase.startsWith('REVISE')}></textarea></label>{/if}<div class="actions three"><button class="danger" onclick={() => decideReview('REJECT')} disabled={busy}>{ui('review.reject')}</button><button onclick={() => decideReview('REVISE')} disabled={busy}>{ui('review.requestRevision')}</button><button class="primary" onclick={() => decideReview('APPROVE')} disabled={busy}>{ui('review.approveEvidence')}</button></div>{/if}{:else}<div class="empty">{ui('review.select')}</div>{/if}</div>
      </section>
    {:else if section === 'governance'}
      {#if governance}
        <section class="metrics">
          <div><span>{ui('governance.critical')}</span><strong>{governance.summary.critical}</strong><small>{ui('governance.criticalHelp')}</small></div>
          <div><span>{ui('governance.high')}</span><strong>{governance.summary.high}</strong><small>{ui('governance.highHelp')}</small></div>
          <div><span>{ui('governance.rules')}</span><strong>{governance.summary.rules_executed}</strong><small>{ui('governance.openFindingsCount', { count: governance.summary.findings })}</small></div>
        </section>
        <section class="panel">
          <div class="panel-title"><div><p class="eyebrow">{ui('governance.inspectionEyebrow')}</p><h2>{ui('governance.findingsTitle')}</h2></div><span class="status">{safeDisplay(governance.integrity.verification)}</span></div>
          <p class="boundary-note">{safeDisplay(governance.boundary.authority)}</p>
          <dl><div><dt>{ui('governance.observed')}</dt><dd>{safeDisplay(governance.observed_at)}</dd></div><div><dt>{ui('governance.ledgerHead')}</dt><dd class="mono">{safeDisplay(governance.integrity.ledger_event_id)}</dd></div><div><dt>{ui('governance.reportDigest')}</dt><dd class="mono">{safeDisplay(governance.sha256)}</dd></div></dl>
        </section>
        <section class="split">
          <div class="panel list"><div class="panel-title"><div><p class="eyebrow">{ui('governance.ruleHostEyebrow')}</p><h2>{ui('governance.openFindings')}</h2></div><span class="count">{governance.findings.length}</span></div>{#if governance.findings.length}{#each governance.findings as finding}<button class:selected={selectedFinding?.id === finding.id} onclick={() => selectedFinding = finding}><div><strong>{safeDisplay(finding.rule_id)}</strong><span>{safeDisplay(finding.scope_kind)} · {safeDisplay(finding.scope_id)}</span></div><span class="risk">{safeDisplay(finding.severity)}</span></button>{/each}{:else}<div class="empty">{ui('governance.noFindings')}</div>{/if}</div>
          <div class="panel detail">{#if selectedFinding}<p class="eyebrow">{safeDisplay(selectedFinding.category)}</p><h2>{safeDisplay(selectedFinding.message)}</h2><dl><div><dt>{ui('governance.severity')}</dt><dd>{safeDisplay(selectedFinding.severity)}</dd></div><div><dt>{ui('governance.status')}</dt><dd>{safeDisplay(selectedFinding.status)}</dd></div><div><dt>{ui('governance.scope')}</dt><dd>{safeDisplay(selectedFinding.scope_kind)} {safeDisplay(selectedFinding.scope_id)}</dd></div><div><dt>{ui('governance.rule')}</dt><dd class="mono">{safeDisplay(selectedFinding.rule_id)}</dd></div></dl><h4>{ui('governance.evidenceReferences')}</h4><ul class="mono">{#each selectedFinding.evidence_refs as ref}<li>{safeDisplay(ref)}</li>{/each}</ul><p class="boundary-note">{ui('governance.readOnlyBoundary')}</p>{:else}<div class="empty">{ui('governance.selectFinding')}</div>{/if}</div>
        </section>
      {:else}<div class="panel empty">{ui('governance.openPrompt')}</div>{/if}
    {:else}
      <section class="grid two"><div class="panel"><p class="eyebrow">{ui('system.localBoundary')}</p><h2>{ui('system.sessionTitle')}</h2><dl><div><dt>{ui('system.organization')}</dt><dd>{safeDisplay(identity?.organization ?? ui('system.unavailable'))}</dd></div><div><dt>{ui('system.installMode')}</dt><dd>{safeDisplay(identity?.mode ?? ui('system.unavailable'))}</dd></div><div><dt>{ui('app.title')}</dt><dd>{safeDisplay(identity?.version ?? ui('system.unavailable'))}</dd></div><div><dt>{ui('system.expires')}</dt><dd>{safeDisplay(identity?.session_expires_at ?? ui('system.unavailable'))}</dd></div></dl></div><div class="panel"><p class="eyebrow">{ui('system.diagnostics')}</p><h2>{ui('system.checksTitle')}</h2><p>{ui('system.doctorHelp')}</p><pre>agentos doctor
  sudo agentos doctor</pre></div><div class="panel"><p class="eyebrow">{ui('system.aimsEyebrow')}</p><h2>{ui('system.readinessTitle')}</h2><p>{ui('system.readinessHelp')}</p><button class="primary" onclick={downloadAIMSEvidence} disabled={busy || !identity}>{ui('system.downloadEvidence')}</button><p class="boundary-note">{ui('system.readinessBoundary')}</p></div></section>
    {/if}
  </main>
</div>
