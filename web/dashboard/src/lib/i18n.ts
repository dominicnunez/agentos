const english = {
  'app.title': 'Agent OS',
  'app.description': 'Agent OS organization dashboard',
  'app.brandSubtitle': 'Organization control',
  'navigation.label': 'Dashboard',
  'navigation.overview': 'Overview',
  'navigation.organization': 'Organization',
  'navigation.work': 'Work',
  'navigation.approvals': 'Approvals',
  'navigation.reviews': 'Reviews',
  'navigation.governance': 'Governance',
  'navigation.system': 'System',
  'identity.notConnected': 'Not connected',
  'identity.installation': '{mode} installation',
  'identity.sessionRequired': 'local session required',
  'header.organization': 'Artificial organization',
  'header.refresh': 'Refresh',
  'section.overview': 'Command center',
  'section.organization': 'Organization',
  'section.work': 'Work',
  'section.approvals': 'Approvals',
  'section.reviews': 'Reviews',
  'section.governance': 'Governance',
  'section.system': 'System',
  'overview.activeWork': 'Active Work',
  'overview.organizationCounts': '{missions} Missions · {goals} Goals · {teams} Teams · {agents} Agents',
  'overview.organizationUnavailable': 'Organization state unavailable',
  'overview.approvals': 'Approvals',
  'overview.effectsAwaiting': 'Exact effects awaiting a decision',
  'overview.completionReviews': 'Completion reviews',
  'overview.evidenceAwaiting': 'Evidence awaiting judgment',
  'overview.currentOrganization': 'Current organization',
  'overview.connect': 'Connect to Agent OS',
  'overview.organizationSummary': 'Durable Missions provide direction, Goals define measurable outcomes, and bounded Work becomes Task DAGs assigned to reviewed Agents.',
  'overview.openOrganization': 'Open organization',
  'overview.mission': 'Mission',
  'overview.goal': 'Goal',
  'overview.work': 'Work',
  'overview.task': 'Task',
  'overview.continueWork': 'Continue work',
  'overview.openWork': 'Open work',
  'overview.intakeConversation': 'Intake conversation',
  'overview.noIntake': 'No unfinished intake conversation.',
  'overview.governanceQueue': 'Governance queue',
  'overview.effectDecisions': 'effect decisions',
  'overview.evidenceReviews': 'evidence reviews',
  'organization.setupEyebrow': 'Set durable direction',
  'organization.setupTitle': 'Create a Mission and measurable Goal',
  'organization.setupSummary': 'Mission is enduring direction. Goal is a target or continuous outcome that Work can be bound to.',
  'organization.missionLabel': 'Mission',
  'organization.missionPlaceholder': 'The enduring purpose this organization should pursue.',
  'organization.goalLabel': 'Goal',
  'organization.goalPlaceholder': 'A measurable outcome under this Mission.',
  'organization.modeLabel': 'Mode',
  'organization.target': 'Target',
  'organization.continuous': 'Continuous',
  'organization.successCriteria': 'Success criteria',
  'organization.successCriteriaPlaceholder': 'One required result per line.',
  'organization.successCriteriaHelp': 'Every line is required. Work cannot mark a target achieved without durable completion evidence.',
  'organization.createStrategy': 'Create Mission and Goal',
  'organization.strategyBoundary': 'This sets organizational direction. It grants no effect permission, approval, capability, policy, or completion authority.',
  'organization.durableDirection': 'Durable direction',
  'organization.policy': 'Policy {version}',
  'organization.projectionBoundary': 'This is a read-only tenant-scoped projection. It grants no authority and exposes no model instructions, credentials, tools, event payloads, or private execution context.',
  'organization.revision': '{id} · revision {version}',
  'organization.goalEyebrow': 'Goal · {mode}',
  'organization.replaces': 'Replaces {id}',
  'organization.assignedTo': 'Assigned to {type} {id}',
  'organization.parent': 'Parent {id}',
  'organization.dependsOn': 'Depends on {ids}',
  'organization.noWorkForGoal': 'No Work is currently bound to this Goal.',
  'organization.noGoalsForMission': 'No Goals are currently bound to this Mission.',
  'organization.noMission': 'No durable Mission has been created yet.',
  'organization.workWithoutGoal': 'Work without a Goal',
  'organization.unalignedWork': 'Unaligned Work',
  'organization.durableStructure': 'Durable structure',
  'organization.teams': 'Teams',
  'organization.members': 'Members:',
  'organization.none': 'None',
  'organization.noTeams': 'No durable Teams have been admitted.',
  'organization.durableIdentities': 'Durable identities',
  'organization.agents': 'Agents',
  'organization.agent': 'Agent',
  'organization.blueprint': 'Blueprint',
  'organization.profile': 'Profile',
  'organization.model': 'Model',
  'organization.runtime': 'Runtime',
  'organization.noAgents': 'No durable Agents have been admitted.',
  'organization.unavailable': 'Organization state is unavailable.'
} as const;

export type DisplayLocale = 'en';
export type DisplayMessageID = keyof typeof english;
export type DisplayMessageValues = Readonly<Record<string, string | number>>;

const catalogs: Readonly<Record<DisplayLocale, Readonly<Record<DisplayMessageID, string>>>> = {
  en: english
};

const localePattern = /^[a-z]{2,3}(?:-[a-z0-9]{2,8})*$/i;
const placeholderPattern = /\{([a-z][a-z0-9_]*)\}/gi;

export function resolveDisplayLocale(candidates: readonly string[]): DisplayLocale {
  for (const candidate of candidates) {
    const normalized = candidate.trim().toLowerCase();
    if (!localePattern.test(normalized)) continue;
    if (normalized === 'en' || normalized.startsWith('en-')) return 'en';
  }
  return 'en';
}

export function formatDisplayMessage(
  locale: DisplayLocale,
  id: DisplayMessageID,
  values: DisplayMessageValues = {}
): string {
  const template = catalogs[locale][id];
  const required = new Set<string>();
  for (const match of template.matchAll(placeholderPattern)) required.add(match[1]);
  const supplied = Object.keys(values);
  if (supplied.length !== required.size || supplied.some((key) => !required.has(key))) {
    throw new Error(`invalid display message values for ${id}`);
  }
  return template.replace(placeholderPattern, (_placeholder, key: string) => {
    if (!Object.hasOwn(values, key)) throw new Error(`missing display message value for ${id}`);
    return String(values[key]);
  });
}
