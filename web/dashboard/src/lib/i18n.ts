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
  'overview.evidenceReviews': 'evidence reviews'
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
