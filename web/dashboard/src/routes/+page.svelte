<script lang="ts">
  import { onMount } from 'svelte';
  import { APIError, api, connect, emptyJSONPost, identifier } from '$lib/api';
  import { approvalRetryBinding, completionReviewFeedback, confirmationMessageID, confirmationRetryBinding, discardConfirmationRetry, discardStrategyRetry, loadAllCompletionReviews, matchesConfirmationRetry, matchesStrategyRetry, parseApprovalRetryBinding, parseConfirmationRetryBinding, parseReviewRetryBinding, parseStrategyRetryBinding, replayApprovalDecision, replayCompletionReviewDecision, reviewRetryBinding, safeDisplay, sameCompletionContract, snapshotCompletionEvidence, strategyRetryBinding, terminalApproval, terminalCompletionReview, validateArtifactSelections, validateCompletionFields } from '$lib/governance';
  import type { ApprovalRetryBinding, ReviewRetryBinding, StrategyRetryBinding } from '$lib/governance';
  import '$lib/app.css';
  import type { Approval, CompletionReview, CompletionReviewPage, DashboardIdentity, IntentDraft, OrganizationSnapshot, TaskView } from '$lib/types';

  type Section = 'overview' | 'organization' | 'work' | 'approvals' | 'reviews' | 'system';
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

  let section: Section = 'overview';
  let identity: DashboardIdentity | null = null;
  let organization: OrganizationSnapshot | null = null;
  let organizationIndex = emptyOrganizationIndex();
  let active: TaskView | null = null;
  let approvals: Approval[] = [];
  let reviews: CompletionReview[] = [];
  let selectedApproval: Approval | null = null;
  let selectedReview: CompletionReview | null = null;
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
          notice = 'Recovered durable Mission and Goal creation after an interrupted response.';
        }
      } catch (cause) {
        recoveryFailures.push(message(cause));
      }
      try {
        const recovered = await recoverPendingConfirmation();
        if (recovered) {
          task = recovered;
          taskID = recovered.task_id;
          notice = `Recovered Task ${recovered.task_id} after an interrupted Intent confirmation.`;
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
      if (recoveryFailures.length) error = `Pending operation recovery failed. ${recoveryFailures.join(' ')}${error ? ` ${error}` : ''}`;
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
          if (recorded !== pendingApprovalDecision.decision) notice = `Another authorized operator recorded ${recovered.status} for the recovered approval.`;
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
          if (recovered.state !== pendingReviewDecision.decision || (recovered.state !== 'APPROVE' && recovered.feedback !== pendingReviewDecision.feedback)) notice = `Another authorized operator recorded ${recovered.state} for the recovered completion review.`;
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
    if (failures.length) error = `Dashboard refresh failed; previously loaded governance data was preserved. ${failures.join(' ')}`;
    selectedApproval = approvals.find((item) => item.approval_id === selectedApproval?.approval_id) ?? (selectedApproval && (terminalApproval(selectedApproval) || pendingApprovalDecision) ? selectedApproval : null);
    selectedReview = reviews.find((item) => item.review_id === selectedReview?.review_id) ?? (selectedReview && (terminalCompletionReview(selectedReview) || pendingReviewDecision) ? selectedReview : null);
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
        notice = 'The interrupted approval expired without authorizing its effect.';
        return available;
      }
      throw cause;
    }
    if (exact.approval_id !== pendingApprovalDecision.approval_id || exact.effect_fingerprint !== pendingApprovalDecision.fingerprint) throw new Error('The durable approval changed while recovering a decision.');
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
      throw new Error('The durable completion review changed while recovering a decision.');
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
      notice = active.prompt || 'The proposed work was updated.';
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
      notice = `Task ${confirmed.task_id} for Work ${confirmed.work_id || ''} was created from the confirmed Intent.`;
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
      notice = 'The intake was abandoned. Its event history remains immutable.';
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
    return Boolean(identity && missionStatement.trim() && goalObjective.trim() && strategyCriteria().length);
  }

  async function bootstrapStrategy(): Promise<void> {
    const statement = missionStatement.trim();
    const objective = goalObjective.trim();
    const criteria = strategyCriteria();
    if (!statement || !objective || !criteria.length) return;
    if (pendingStrategy && !matchesStrategyRetry(pendingStrategy, statement, objective, goalMode, criteria)) {
      error = 'The interrupted Mission and Goal creation must be recovered before different direction can be submitted.';
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
      notice = 'The Mission and Goal are now durable organizational direction.';
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
      notice = task.prompt || 'The requested input was recorded and work resumed.';
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
    if (pendingApprovalDecision) {
      error = 'The earlier approval decision must be recovered before another decision can begin.';
      return;
    }
    const approval = selectedApproval;
    if (terminalApproval(approval)) return;
    const expected = decision === 'APPROVE' ? `APPROVE ${approval.effect_fingerprint.slice(0, 12)}` : 'DENY';
    if (approvalPhrase.trim() !== expected) {
      error = `Type ${expected} exactly.`;
      return;
    }
    pendingApprovalDecision = approvalRetryBinding(approval.approval_id, approval.effect_fingerprint, decision);
    sessionStorage.setItem(pendingApprovalKey, JSON.stringify(pendingApprovalDecision));
    await action(async () => {
      let current = await api<Approval>(`/api/v1/control/approvals/${encodeURIComponent(approval.approval_id)}`);
      if (current.effect_fingerprint !== approval.effect_fingerprint) throw new Error('The durable approval fingerprint changed.');
      if (terminalApproval(current)) {
        const recorded = current.status === 'APPROVED' ? 'APPROVE' : 'DENY';
        if (recorded !== decision) throw new Error(`The exact effect was already ${current.status.toLowerCase()}.`);
        selectedApproval = current;
        clearPendingApprovalDecision();
        approvalPhrase = '';
        notice = `Exact effect ${decision === 'APPROVE' ? 'approved' : 'denied'}.`;
        await refresh();
        return;
      }
      selectedApproval = await replayApprovalDecision(api, current, decision);
      clearPendingApprovalDecision();
      approvalPhrase = '';
      notice = `Exact effect ${decision === 'APPROVE' ? 'approved' : 'denied'}.`;
      await refresh();
    });
  }

  async function decideReview(decision: 'APPROVE' | 'REJECT' | 'REVISE'): Promise<void> {
    if (!selectedReview) return;
    if (pendingReviewDecision) {
      error = 'The earlier completion-review decision must be recovered before another decision can begin.';
      return;
    }
    const review = selectedReview;
    if (terminalCompletionReview(review)) return;
    const expected = `${decision} ${review.fingerprint.slice(0, 12)}`;
    if (reviewPhrase.trim() !== expected) {
      error = `Type ${expected} exactly.`;
      return;
    }
    if (decision === 'REVISE' && !reviewFeedback.trim()) {
      error = 'Revision feedback is required.';
      return;
    }
    const feedback = completionReviewFeedback(decision, reviewFeedback);
    pendingReviewDecision = reviewRetryBinding(review.task_id, review.review_id, review.fingerprint, decision, feedback);
    sessionStorage.setItem(pendingReviewKey, JSON.stringify(pendingReviewDecision));
    await action(async () => {
      const current = await api<CompletionReview>(`/api/v1/user/reviews/${encodeURIComponent(review.task_id)}/records/${encodeURIComponent(review.review_id)}`);
      if (current.review_id !== review.review_id || current.fingerprint !== review.fingerprint) throw new Error('The durable completion review changed.');
      if (terminalCompletionReview(current)) {
        if (current.state !== decision || (decision !== 'APPROVE' && current.feedback !== feedback)) throw new Error('The completion review already has a different durable decision.');
        selectedReview = current;
        clearPendingReviewDecision();
        reviewPhrase = '';
        reviewFeedback = '';
        notice = `Completion evidence is already marked ${decision.toLowerCase()}.`;
        await refresh();
        return;
      }
      selectedReview = await replayCompletionReviewDecision(api, current, pendingReviewDecision!);
      clearPendingReviewDecision();
      reviewPhrase = '';
      reviewFeedback = '';
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
    return cause instanceof Error ? safeDisplay(cause.message) : 'The request failed.';
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
</script>

<svelte:head>
  <title>Agent OS</title>
  <meta name="description" content="Agent OS organization dashboard" />
</svelte:head>

<div class="shell">
  <aside>
    <div class="brand"><span class="mark">AO</span><div><strong>Agent OS</strong><small>Organization control</small></div></div>
    <nav aria-label="Dashboard">
      {#each [['overview','Overview'],['organization','Organization'],['work','Work'],['approvals','Approvals'],['reviews','Reviews'],['system','System']] as item}
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
        <button onclick={() => section='organization'}><span>Active Work</span><strong>{organizationIndex.activeWorkCount}</strong><small>{organization ? `${organization.missions.length} Missions · ${organization.goals.length} Goals · ${organization.teams.length} Teams · ${organization.agents.length} Agents` : 'Organization state unavailable'}</small></button>
        <button onclick={() => section='approvals'}><span>Approvals</span><strong>{pendingApprovalCount()}</strong><small>Exact effects awaiting a decision</small></button>
        <button onclick={() => section='reviews'}><span>Completion reviews</span><strong>{pendingReviewCount()}</strong><small>Evidence awaiting judgment</small></button>
      </section>
      <section class="panel mission">
        <div><p class="eyebrow">Current organization</p><h2>{safeDisplay(organization?.organization.name ?? identity?.organization ?? 'Connect to Agent OS')}</h2><p>Durable Missions provide direction, Goals define measurable outcomes, and bounded Work becomes Task DAGs assigned to reviewed Agents.</p><button class="text" onclick={() => section='organization'}>Open organization</button></div>
        <div class="flow"><span>Mission</span><i></i><span>Goal</span><i></i><span>Work</span><i></i><span>Task</span></div>
      </section>
      <section class="grid two">
        <div class="panel"><div class="panel-title"><h2>Continue work</h2><button class="text" onclick={() => section='work'}>Open work</button></div>{#if active}<span class="status">{safeDisplay(active.state)}</span><h3>{safeDisplay(active.intent?.objective ?? active.prompt ?? 'Intake conversation')}</h3><p class="mono">{safeDisplay(active.conversation_id)}</p>{:else}<div class="empty">No unfinished intake conversation.</div>{/if}</div>
        <div class="panel"><div class="panel-title"><h2>Governance queue</h2></div><div class="queue"><div><strong>{pendingApprovalCount()}</strong><span>effect decisions</span></div><div><strong>{pendingReviewCount()}</strong><span>evidence reviews</span></div></div></div>
      </section>
    {:else if section === 'organization'}
      <section class="panel strategy-setup">
        <div><p class="eyebrow">Set durable direction</p><h2>Create a Mission and measurable Goal</h2><p>Mission is enduring direction. Goal is a target or continuous outcome that Work can be bound to.</p></div>
        <div class="strategy-fields">
          <label>Mission<textarea bind:value={missionStatement} disabled={busy} rows="3" placeholder="The enduring purpose this organization should pursue."></textarea></label>
          <label>Goal<input bind:value={goalObjective} disabled={busy} placeholder="A measurable outcome under this Mission." /></label>
          <label>Mode<select bind:value={goalMode} disabled={busy}><option value="TARGET">Target</option><option value="CONTINUOUS">Continuous</option></select></label>
          <label>Success criteria<textarea bind:value={goalCriteria} disabled={busy} rows="4" placeholder="One required result per line."></textarea><small>Every line is required. Work cannot mark a target achieved without durable completion evidence.</small></label>
          <button class="primary" onclick={bootstrapStrategy} disabled={busy || !canBootstrapStrategy()}>Create Mission and Goal</button>
          <p class="boundary-note">This sets organizational direction. It grants no effect permission, approval, capability, policy, or completion authority.</p>
        </div>
      </section>
      {#if organization}
        <section class="organization-layout">
          <div class="panel organization-tree">
            <div class="panel-title"><div><p class="eyebrow">Durable direction</p><h2>{safeDisplay(organization.organization.name)}</h2></div><span class="status">Policy {safeDisplay(organization.organization.policy_version)}</span></div>
            <p class="boundary-note">This is a read-only tenant-scoped projection. It grants no authority and exposes no model instructions, credentials, tools, event payloads, or private execution context.</p>
            {#if organization.missions.length}
              {#each organization.missions as mission}
                {@const missionGoals = organizationIndex.goalsByMission.get(mission.id) ?? []}
                <article class="mission-card">
                  <div class="organization-heading"><div><p class="eyebrow">Mission</p><h3>{safeDisplay(mission.statement)}</h3><small class="mono">{safeDisplay(mission.id)} · revision {mission.version}</small></div><span class="status">{safeDisplay(mission.status)}</span></div>
                  {#if missionGoals.length}
                    {#each missionGoals as goal}
                      {@const goalWorks = organizationIndex.worksByGoal.get(goal.id) ?? []}
                      <div class="goal-card">
                        <div class="organization-heading"><div><p class="eyebrow">Goal · {safeDisplay(goal.mode)}</p><h3>{safeDisplay(goal.objective)}</h3><small class="mono">{safeDisplay(goal.id)} · revision {goal.version}</small></div><span class="status">{safeDisplay(goal.status)}</span></div>
                        {#if goal.success_criteria.length}<h4>Success criteria</h4><ul>{#each goal.success_criteria as criterion}<li>{safeDisplay(criterion)}</li>{/each}</ul>{/if}
                        {#if goalWorks.length}
                          {#each goalWorks as work}
                            {@const workTasks = organizationIndex.tasksByWork.get(work.id) ?? []}
                            <div class="work-card"><div class="organization-heading"><div><strong>{safeDisplay(work.objective)}</strong><small class="mono">{safeDisplay(work.id)}</small>{#if work.replaces_work_id}<small>Replaces {safeDisplay(work.replaces_work_id)}</small>{/if}</div><span class="status">{safeDisplay(work.status)}</span></div>
                              <small>{safeDisplay(work.mode)}{work.experiment_status ? ` · ${safeDisplay(work.experiment_status)}` : ''}</small>{#if work.trust_label}<span class="risk">{safeDisplay(work.trust_label)}</span>{/if}
                              {#if workTasks.length}<div class="task-dag">{#each workTasks as item}<div><span class="task-node"></span><p><strong>{safeDisplay(item.description)}</strong><small>{safeDisplay(item.execution_kind)} · {safeDisplay(item.model_inference_policy)} · {safeDisplay(item.status)}</small>{#if item.assignee_id}<small>Assigned to {safeDisplay(item.assignee_type ?? 'UNKNOWN')} {safeDisplay(item.assignee_id)}</small>{/if}{#if item.parent_id}<small>Parent {safeDisplay(item.parent_id)}</small>{/if}{#if item.depends_on.length}<small>Depends on {item.depends_on.map(safeDisplay).join(', ')}</small>{/if}</p></div>{/each}</div>{/if}
                            </div>
                          {/each}
                        {:else}<div class="organization-empty">No Work is currently bound to this Goal.</div>{/if}
                      </div>
                    {/each}
                  {:else}<div class="organization-empty">No Goals are currently bound to this Mission.</div>{/if}
                </article>
              {/each}
            {:else}<div class="organization-empty">No durable Mission has been created yet.</div>{/if}
            {#if organizationIndex.unalignedWorks.length}<article class="mission-card"><p class="eyebrow">Work without a Goal</p><h3>Unaligned Work</h3>{#each organizationIndex.unalignedWorks as work}{@const workTasks = organizationIndex.tasksByWork.get(work.id) ?? []}<div class="work-card"><div class="organization-heading"><div><strong>{safeDisplay(work.objective)}</strong><small class="mono">{safeDisplay(work.id)}</small>{#if work.replaces_work_id}<small>Replaces {safeDisplay(work.replaces_work_id)}</small>{/if}</div><span class="status">{safeDisplay(work.status)}</span></div><small>{safeDisplay(work.mode)}{work.experiment_status ? ` · ${safeDisplay(work.experiment_status)}` : ''}</small>{#if work.trust_label}<span class="risk">{safeDisplay(work.trust_label)}</span>{/if}{#if workTasks.length}<div class="task-dag">{#each workTasks as item}<div><span class="task-node"></span><p><strong>{safeDisplay(item.description)}</strong><small>{safeDisplay(item.execution_kind)} · {safeDisplay(item.model_inference_policy)} · {safeDisplay(item.status)}</small>{#if item.assignee_id}<small>Assigned to {safeDisplay(item.assignee_type ?? 'UNKNOWN')} {safeDisplay(item.assignee_id)}</small>{/if}{#if item.parent_id}<small>Parent {safeDisplay(item.parent_id)}</small>{/if}{#if item.depends_on.length}<small>Depends on {item.depends_on.map(safeDisplay).join(', ')}</small>{/if}</p></div>{/each}</div>{/if}</div>{/each}</article>{/if}
          </div>
          <div class="organization-roster">
            <div class="panel agent-roster"><div class="panel-title"><div><p class="eyebrow">Durable structure</p><h2>Teams</h2></div><span class="count">{organization.teams.length}</span></div>{#if organization.teams.length}{#each organization.teams as team}<article><div class="organization-heading"><div><h3>{safeDisplay(team.name)}</h3><small class="mono">{safeDisplay(team.id)} · revision {team.version}</small></div><span class="status">{safeDisplay(team.status)}</span></div>{#if team.mission}<p>{safeDisplay(team.mission)}</p>{/if}<p><strong>Members:</strong> {team.member_agent_ids.length ? team.member_agent_ids.map(safeDisplay).join(', ') : 'None'}</p></article>{/each}{:else}<div class="organization-empty">No durable Teams have been admitted.</div>{/if}</div>
            <div class="panel agent-roster"><div class="panel-title"><div><p class="eyebrow">Durable identities</p><h2>Agents</h2></div><span class="count">{organization.agents.length}</span></div>{#if organization.agents.length}{#each organization.agents as agent}<article><div class="organization-heading"><div><h3>{safeDisplay(agent.role)}</h3><small class="mono">{safeDisplay(agent.id)} · revision {agent.version}</small></div><span class:offline={!agent.available} class="status">{agent.available ? 'AVAILABLE' : 'UNAVAILABLE'}</span></div><dl><div><dt>Agent</dt><dd>{safeDisplay(agent.status)}</dd></div><div><dt>Blueprint</dt><dd>{safeDisplay(agent.blueprint_status)}</dd></div><div><dt>Profile</dt><dd>{safeDisplay(agent.execution_profile_status)}</dd></div><div><dt>Model</dt><dd>{safeDisplay(agent.model_provider)} / {safeDisplay(agent.model)}</dd></div><div><dt>Runtime</dt><dd>{safeDisplay(agent.runtime_adapter)}</dd></div></dl></article>{/each}{:else}<div class="organization-empty">No durable Agents have been admitted.</div>{/if}</div>
          </div>
        </section>
      {:else}<div class="panel empty">Organization state is unavailable.</div>{/if}
    {:else if section === 'work'}
      <section class="grid work-grid">
        <div class="panel composer"><p class="eyebrow">Natural-language intake</p><h2>{active ? 'Continue the conversation' : 'What should the organization accomplish?'}</h2><textarea bind:value={workText} disabled={busy} rows="7" placeholder="Describe the outcome, relevant context, constraints, and anything only you can provide."></textarea><label>Goal<select bind:value={selectedGoalID} disabled={busy || Boolean(active)}><option value="">Ad hoc work</option>{#each selectableGoals() as goal}<option value={goal.id}>{safeDisplay(goal.objective)} · {safeDisplay(goal.id)}</option>{/each}</select><small>Choosing a Goal binds the reviewed Work to that existing durable objective. It grants no new authority.</small></label><label>Execution<select bind:value={executionKind} disabled={busy || Boolean(active)}><option value="">Automatic</option><option value="HUMAN">User task</option></select><small>Automatic prefers deterministic work and uses an Agent only when justified.</small></label><div class="actions"><button class="primary" onclick={submitWork} disabled={busy || !identity || !workText.trim()}>Send</button><small>The model proposes a bounded Intent. Nothing starts until you confirm the exact review.</small></div></div>
        <div class="panel intent"><div class="panel-title"><div><p class="eyebrow">Intent contract</p><h2>Review before work begins</h2></div>{#if active?.state}<span class="status">{safeDisplay(active.state)}</span>{/if}</div>
          {#if active?.intent}
            {#if active.prompt}<div class="banner notice governed-text" role="status"><strong>{active.state === 'INPUT_REQUIRED' ? 'More information required' : 'Review guidance'}</strong><br />{safeDisplay(active.prompt)}</div>{/if}
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
          {#if active}<button class="danger wide" onclick={abandonIntent} disabled={busy}>Abandon intake</button><p class="boundary-note">Abandoning closes only this unconfirmed intake. It does not delete its durable history or affect existing Work.</p>{/if}
        </div>
      </section>
      <section class="panel task-lookup"><div><p class="eyebrow">Durable status</p><h2>Find a Task</h2></div><div class="inline"><input bind:value={taskID} placeholder="task-id" /><button onclick={findTask} disabled={busy || !taskID.trim()}>Open</button></div>
        {#if task}<div class="task"><div><span class="status">{safeDisplay(task.state)}</span><h3>{safeDisplay(task.task_id)}</h3>{#if task.work_id}<p class="mono">Work {safeDisplay(task.work_id)}</p>{/if}{#if task.mode}<p>Mode: <strong>{safeDisplay(task.mode)}</strong></p>{/if}{#if task.trust_label}<p class="risk">Trust: {safeDisplay(task.trust_label)}</p>{/if}{#if task.prompt}<p class="boundary-note governed-text">{safeDisplay(task.prompt)}</p>{/if}{#if task.result}<p class="governed-text">{safeDisplay(task.result)}</p>{/if}</div>
          {#if task.completion_recovery_required}<div class="empty"><p>Previously submitted completion evidence is awaiting durable recovery.</p><button onclick={() => refresh()} disabled={busy}>Retry recovery</button></div>{:else if task.input_recovery_required}<div class="empty"><p>Previously submitted input is awaiting durable recovery.</p><button onclick={() => refresh()} disabled={busy}>Retry recovery</button></div>{:else if task.review_required}<div class="empty"><p>This Task is waiting for an independent completion judgment.</p><button class="text" onclick={() => section='reviews'}>Open Reviews</button></div>{:else if task.completion_contract}{#key `${task.task_id}:${task.completion_contract.task_version}`}<form onsubmit={(event) => { event.preventDefault(); submitCompletion(); }}><h4>Required completion evidence</h4>{#each task.completion_contract.required_fields ?? [] as field}<label>{safeDisplay(field.name)}<small>{safeDisplay(field.description)}; {field.min_bytes} to {field.max_bytes} UTF-8 bytes</small><textarea required disabled={busy} value={completionFields[field.name] ?? ''} oninput={(event) => setCompletionField(field.name, event.currentTarget.value)}></textarea></label>{/each}{#each task.completion_contract.artifact_requirements ?? [] as requirement}<label>{safeDisplay(requirement.role)}<small>{requirement.min_count} to {requirement.max_count} files; {requirement.media_types.map(safeDisplay).join(', ')}</small><input type="file" disabled={busy} required={requirement.min_count > 0} multiple={requirement.max_count > 1} accept={requirement.media_types.join(',')} onchange={(event) => setFiles(requirement.role, event)} /></label>{/each}<button class="primary" type="submit" disabled={busy}>Submit complete evidence</button></form>{/key}{:else if task.state === 'INPUT_REQUIRED' && task.conversation_id && task.user_input_allowed}<form onsubmit={(event) => { event.preventDefault(); submitTaskInput(); }}><h4>Provide requested input</h4><label>Response<textarea bind:value={taskInput} disabled={busy} required placeholder="Provide the information requested above."></textarea></label><button class="primary" type="submit" disabled={busy || !taskInput.trim()}>Continue Task</button></form>{/if}
        </div>{/if}
      </section>
    {:else if section === 'approvals'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">Consequential effects</p><h2>Approvals</h2></div><span class="count">{pendingApprovalCount()}</span></div>{#if approvals.length}{#each approvals as approval}<button class:selected={selectedApproval?.approval_id === approval.approval_id} onclick={() => {selectedApproval=approval; approvalPhrase='';}}><div><strong>{safeDisplay(approval.action)}</strong><span>{safeDisplay(approval.resource)}</span></div><div><span class="risk">{safeDisplay(approval.risk)}</span><small>{safeDisplay(approval.status)} · {safeDisplay(approval.boundary)}</small></div></button>{/each}{:else}<div class="empty">No pending or recent approval decisions.</div>{/if}</div>
        <div class="panel detail">{#if selectedApproval}<p class="eyebrow">Exact proposed effect</p><h2>{safeDisplay(selectedApproval.canonical_effect_descriptor)}</h2><dl><div><dt>Status</dt><dd>{safeDisplay(selectedApproval.status)}</dd></div><div><dt>Action</dt><dd>{safeDisplay(selectedApproval.action)}</dd></div><div><dt>Resource</dt><dd>{safeDisplay(selectedApproval.resource)}</dd></div><div><dt>Scope</dt><dd>{safeDisplay(selectedApproval.scope)}</dd></div><div><dt>Boundary</dt><dd>{safeDisplay(selectedApproval.boundary)}</dd></div><div><dt>Risk</dt><dd>{safeDisplay(selectedApproval.risk)}</dd></div><div><dt>Urgency</dt><dd>{safeDisplay(selectedApproval.urgency)}</dd></div><div><dt>Expires</dt><dd>{safeDisplay(selectedApproval.expires_at ?? 'No expiry recorded')}</dd></div><div><dt>Single use</dt><dd>{selectedApproval.single_use ? 'Yes' : 'No'}</dd></div></dl>{#if Object.keys(selectedApproval.effect_arguments).length}<h4>Arguments</h4><pre>{safeDisplay(JSON.stringify(selectedApproval.effect_arguments, null, 2))}</pre>{/if}<div class="fingerprint"><span>Effect fingerprint</span><code>{selectedApproval.effect_fingerprint}</code></div>{#if selectedApproval.status === 'APPROVED' || selectedApproval.status === 'DENIED'}<p class="boundary-note">The authoritative ledger recorded this exact effect as <strong>{safeDisplay(selectedApproval.status)}</strong>. This decision is immutable.</p>{:else if pendingApprovalDecision}<p class="boundary-note">The earlier approval decision is being recovered. Refresh before deciding another effect.</p><button onclick={() => refresh()} disabled={busy}>Retry recovery</button>{:else}<label>Type <code>APPROVE {selectedApproval.effect_fingerprint.slice(0,12)}</code> or <code>DENY</code><input bind:value={approvalPhrase} autocomplete="off" /></label><div class="actions"><button class="danger" onclick={() => decideApproval('DENY')} disabled={busy}>Deny</button><button class="primary" onclick={() => decideApproval('APPROVE')} disabled={busy}>Approve exact effect</button></div>{/if}{:else}<div class="empty">Select an approval to inspect the immutable effect details.</div>{/if}</div>
      </section>
    {:else if section === 'reviews'}
      <section class="split"><div class="panel list"><div class="panel-title"><div><p class="eyebrow">Completion evidence</p><h2>Completion reviews</h2></div><span class="count">{pendingReviewCount()}</span></div>{#if reviews.length}{#each reviews as review}<button class:selected={selectedReview?.review_id === review.review_id} onclick={() => {selectedReview=review; reviewPhrase=''; reviewFeedback='';}}><div><strong>{safeDisplay(review.objective)}</strong><span>{safeDisplay(review.task_id)}</span></div><span class="status">{safeDisplay(review.state)}</span></button>{/each}{:else}<div class="empty">No pending or recent completion reviews.</div>{/if}</div>
        <div class="panel detail">{#if selectedReview}<p class="eyebrow">Candidate result</p><h2>{safeDisplay(selectedReview.objective)}</h2><blockquote>{safeDisplay(selectedReview.candidate_result ?? selectedReview.result ?? 'No text result supplied.')}</blockquote><h4>Done when</h4><ul>{#each selectedReview.criteria as criterion}<li>{safeDisplay(criterion.description)}</li>{/each}</ul><h4>Evidence references</h4><ul class="mono">{#each selectedReview.evidence_refs as ref}<li>{safeDisplay(ref)}</li>{/each}</ul><div class="fingerprint"><span>Evidence fingerprint</span><code>{selectedReview.fingerprint}</code></div><p class="boundary-note">This judgment verifies the recorded candidate only. It does not approve any consequential effect.</p>{#if selectedReview.state !== 'PENDING'}<dl><div><dt>Decision</dt><dd>{safeDisplay(selectedReview.state)}</dd></div><div><dt>Reviewer</dt><dd>{safeDisplay(selectedReview.reviewer_id ?? 'No reviewer recorded')}</dd></div></dl>{#if selectedReview.feedback}<h4>Recorded feedback</h4><pre>{safeDisplay(selectedReview.feedback)}</pre>{/if}<p class="boundary-note">The authoritative ledger recorded this completion-review decision. It is immutable.</p>{:else if pendingReviewDecision}<p class="boundary-note">The earlier completion-review decision is being recovered. Refresh before beginning another judgment.</p><button onclick={() => refresh()} disabled={busy}>Retry recovery</button>{:else}<label>Type <code>APPROVE {selectedReview.fingerprint.slice(0,12)}</code>, <code>REJECT {selectedReview.fingerprint.slice(0,12)}</code>, or <code>REVISE {selectedReview.fingerprint.slice(0,12)}</code><input bind:value={reviewPhrase} autocomplete="off" /></label>{#if reviewPhrase.startsWith('REJECT') || reviewPhrase.startsWith('REVISE')}<label>Feedback<textarea bind:value={reviewFeedback} required={reviewPhrase.startsWith('REVISE')}></textarea></label>{/if}<div class="actions three"><button class="danger" onclick={() => decideReview('REJECT')} disabled={busy}>Reject</button><button onclick={() => decideReview('REVISE')} disabled={busy}>Request revision</button><button class="primary" onclick={() => decideReview('APPROVE')} disabled={busy}>Approve evidence</button></div>{/if}{:else}<div class="empty">Select a review to compare the candidate result with its exact completion contract.</div>{/if}</div>
      </section>
    {:else}
      <section class="grid two"><div class="panel"><p class="eyebrow">Local boundary</p><h2>Dashboard session</h2><dl><div><dt>Organization</dt><dd>{safeDisplay(identity?.organization ?? 'Unavailable')}</dd></div><div><dt>Install mode</dt><dd>{safeDisplay(identity?.mode ?? 'Unavailable')}</dd></div><div><dt>Agent OS</dt><dd>{safeDisplay(identity?.version ?? 'Unavailable')}</dd></div><div><dt>Expires</dt><dd>{safeDisplay(identity?.session_expires_at ?? 'Unavailable')}</dd></div></dl></div><div class="panel"><p class="eyebrow">Diagnostics</p><h2>Read-only system checks</h2><p>Use <code>agentos doctor</code> for configuration, credential, service, private-gateway, and SQLite integrity checks.</p><pre>agentos doctor
sudo agentos doctor</pre></div></section>
    {/if}
  </main>
</div>
