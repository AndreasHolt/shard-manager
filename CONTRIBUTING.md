# Contributing to Shard Manager

Thanks for considering a contribution. Shard Manager (shard-distributor) is part of the
[Cadence](https://cadenceworkflow.io) project.

> 📚 **New to contributing to Cadence?** Check out the
> [Contributing Guide](https://cadenceworkflow.io/community/how-to-contribute/getting-started)
> for an overview of the contribution process across all Cadence repositories. Development
> setup and conventions specific to this repository live in [AGENTS.md](AGENTS.md).

Looking for something to work on? Start with issues labeled
[good-first-issue](https://github.com/cadence-workflow/shard-manager/issues?q=is%3Aopen+label%3Agood-first-issue).
Once you are familiar with the codebase, look at issues labeled
[help-wanted](https://github.com/cadence-workflow/shard-manager/issues?q=is%3Aopen+label%3Ahelp-wanted).

## Issue Triage

Every issue goes through three triage states tracked by `triage/*` labels:

| Label | Criteria | Who acts |
|---|---|---|
| `triage/needs-info` | Reporter must supply additional details. These may be reproduction steps or logs (for bugs), or clear acceptance criteria and behaviour (for features), etc. | Reporter |
| `triage/needs-decision` | The issue is understood but needs a TSC or area-owner decision on whether/when to address it. | TSC / maintainers |
| `triage/accepted` | Approved and ready to be picked up for implementation. | Contributors |

**TSC review queue:** [`is:open label:triage/needs-decision`](https://github.com/cadence-workflow/shard-manager/issues?q=is%3Aopen+label%3Atriage%2Fneeds-decision)

Issues filed from a template are automatically checked for completeness. If required sections
are empty, the bot applies `triage/needs-info` and posts a comment listing what's missing. Once
the reporter fills in the details, the bot removes the label on the next edit.

## Pull Requests

Before opening a PR, run `make pr` to regenerate code, lint and format, and read
[`.github/pull_request_guidance.md`](.github/pull_request_guidance.md) for what reviewers
expect in the description. PR titles follow
[Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `chore:`, ...).
