# CLAUDE.md

Guidance for Claude Code working in this repository.

## Review split: mechanical vs. architectural

The human reviewer on this project reviews **architecture and functional/
behavioral test correctness** — not code style, test structure, or hardcoded
literals. Claude owns mechanical conformance completely and self-checks it
*before* asking for review, not after being told about it in a comment.

Concretely, before declaring any task, PR, or plan phase done — as an actual
step you execute, not a stated intention. (First attempt at this rule
skipped straight from implementing to running tests to pushing, on both a
9-comment and a 3-comment fix round, without the self-review step below
ever actually running — writing the rule down did not make it happen.)

1. **Self-invoke `/code-review` on your own diff, or at minimum grep the
   diff yourself for repeated literals.** For every test method you wrote or
   touched, check: does any value asserted on also appear, re-typed, in that
   method's Arrange/Act instead of referenced from a variable or constant?
   That single check catches most of what a human reviewer would otherwise
   have to type out by hand.
2. **Check for the whole family of non-invocable rubric skills, not one at a
   time as they happen to come up.** `software-engineer-plugin` ships (at
   least): `testing-standards` and `code-style-standards`, both `paths:`
   -scoped (test files; all source files, respectively — `code-style-standards`
   lives under `develop/`, meaning apply it *while writing*, not only at
   review time) — and `security-standards` / `performance-standards`, which
   have **no** `paths:` field, so they can't be glob-matched: recognize them
   by topic instead (auth/secrets/tokens/CORS → security; DB queries/
   repositories/N+1 → performance). Neither is live in this project yet — no
   auth, no persistence — but check again the moment either gets added.
3. **Never add a new dependency to satisfy a "more idiomatic" fix without
   asking first**, especially if the code already states a deliberate stance
   against it (e.g. this project intentionally has no Boot Kafka starter —
   see `KafkaConsumerConfig`'s `@EnableKafka` comment). Surface the tradeoff,
   let the human decide.

## Established code conventions (self-check these, don't wait to be told)

- **No hardcoded literals shared between production code and the tests
  asserting on them** — config values, Micrometer metric/tag names, and
  ArchUnit's base package all belong in named constants or a shared fixture,
  never duplicated as string literals in two places.
- **Comments are terse and purpose-only** — one line stating the non-obvious
  *why*. Investigation detail, root-cause narrative, and "how we got here"
  belong in the commit message, not inline in code.
- **Test structure**: `@DisplayName` (class and method, business language),
  `@Nested` classes grouped per scenario, explicit `// Arrange` / `// Act` /
  `// Assert` comments, no inline fully-qualified class names (import
  instead), and test data built via a shared `fixture.*` Object Mother class
  per domain rather than inline literals duplicated across test files.
- **Prefer a project-owned `@ConfigurationProperties` record** over scattered
  `@Value` parameters when more than one class needs the same config keys.
- Don't introduce a new test framework (e.g. Cucumber/BDD) or a new
  dependency without an explicit, fresh ask — these are project-wide tooling
  decisions, not per-file style choices.
- **Naming**: intent-revealing names, no abbreviations beyond universally
  understood ones (`id`, `url`, `api`, `dto`), booleans/predicates prefixed
  `is`/`has`/`can`/`should`, verb-phrase method names. Program to the
  abstraction (`Map`/`List`, not `HashMap`/`ArrayList`) in variable/parameter/
  return types. Prefer immutable types (records, `final` fields) by default.

**Known, deliberate exception:** `MessageProcessor.process(...)` /
`SampleMessageProcessor.process(...)` reads as a bare verb, which
`code-style-standards` would flag — left as-is on purpose. It's the
foundational port-interface method name used across the whole hexagonal
design (interface, every implementer, the orchestration service, several
test files); the type name already carries the meaning
(`MessageProcessor.process`), and renaming it is an API-wide churn with no
real clarity gain. Don't re-flag this one without a fresh, explicit reason.

## Working with GitHub, not GitLab

Some installed review/PR skills (`software-engineer-plugin`'s
`gitlab-mr-description`, `addressing-mr-feedback`) default to GitLab's
`glab` CLI and MR/discussions API. This repo is hosted on **GitHub** — follow
the skill's methodology (ground-truth gathering, approval gates before
posting, self-review-MR handling) but substitute `gh` for `glab`, Pull
Request for Merge Request, and use `gh api repos/{owner}/{repo}/pulls/{n}/
comments` for review comments. Resolving a review thread has no simple REST
call — it requires the GraphQL `resolveReviewThread` mutation against the
thread's GraphQL node id (fetched via a `reviewThreads` query), not the REST
comment id.
