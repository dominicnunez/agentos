import type { DisplayMessageID, DisplayMessageValues } from './i18n.ts';

export class DisplayError extends Error {
  readonly messageID: DisplayMessageID;
  readonly values: DisplayMessageValues;

  constructor(messageID: DisplayMessageID, fallback: string, values: DisplayMessageValues = {}) {
    super(fallback);
    this.name = 'DisplayError';
    this.messageID = messageID;
    this.values = values;
  }
}
