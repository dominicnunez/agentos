# Controlled AIMS documented information

This directory holds public project records used to prepare Agent OS for an
ISO/IEC 42001 assessment. It does not establish conformity or certification.
An assessment must use the organization's approved AIMS scope, operating
evidence, an authorized copy of the standard, and an independent assessment
where certification is sought.

## Control boundary

`manifest.json` is the closed index of controlled records. Each entry binds a
stable identifier, exact repository path, version, lifecycle state, owner,
classification, approval evidence, review date, supersession relationship, and
SHA-256 digest. `scripts/verify_aims_documents.py` rejects malformed metadata,
unlisted path forms, symlinks, oversized content, non-UTF-8 or CRLF content,
hash substitution, false draft approval metadata, and incomplete retirement
links.

The lifecycle is:

- `DRAFT`: proposed information with no approval authority;
- `APPROVED`: a user-approved record with an exact decision reference,
  approving identity, approval time, and review date; and
- `RETIRED`: preserved history that identifies its controlled successor.

Code, models, Agents, and CI may prepare and verify drafts. They cannot fill in
approval evidence, approve policy, accept residual risk, determine control
applicability, perform management review, or certify Agent OS. Those decisions
remain explicit and fail closed.

## Public and confidential evidence

Every file committed here is public and must be classified `PUBLIC`. Never put
credentials, tokens, personal data, customer data, private prompts, security
secrets, confidential contracts, or sensitive incident details in this
directory. Confidential operating evidence belongs in an operator-controlled
evidence system with appropriate access, retention, integrity, and redaction
controls. A public record may describe the required evidence class, but must
not copy confidential evidence into the repository.

## Updating a record

1. Change the controlled Markdown document.
2. Increment its manifest version and update its SHA-256 digest.
3. Leave approval fields empty while the record is a draft.
4. Obtain the required user decision before marking it approved or retired.
5. Preserve supersession links so an assessor can follow document history.
6. Run `python3 scripts/verify_aims_documents.py` and its unit tests.

Git history provides reviewable change history, but it does not replace the
manifest lifecycle, operating evidence, effectiveness review, or an assessor's
judgment.

## Public assessment bundle

`scripts/build_aims_assessment_bundle.py` creates a deterministic, bounded
source-commit-specific archive containing the controlled public records, a
curated technical evidence set, exact checksums, and a machine-readable
readiness report. The builder requires the declared commit to equal checked-out
`HEAD` and compares every bundled byte with the corresponding Git object. CI
builds it twice and requires identical bytes. The report
fails the assessment-readiness state while required approved audit, management
review, applicability, and accountable decision records are absent.

The bundle deliberately excludes confidential operating evidence and cannot
authenticate a governance decision merely because approval text appears in a
manifest. Repository protection, review, the exact approval reference, and the
approved confidential evidence system remain part of that trust boundary.
