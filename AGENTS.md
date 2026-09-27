# Project Guidelines

## General Approach

- Keep things simple. Build what is needed now.
- Understand the existing flow and callers before changing code.
- Reuse existing code, the standard library, and installed dependencies before adding anything new.
- Avoid unnecessary abstractions, wrappers, configuration, and boilerplate.
- Fix problems at their source rather than patching individual symptoms.
- Propose substantial simplifications when they would meaningfully improve the project.
- Keep changes focused. Do not mix unrelated cleanup into feature work.

## Rust Style

- Use lowerCamelCase for functions we own. Preserve names required by traits or external APIs.
- Prefer early returns and straightforward control flow.
- Keep functions focused on one clear responsibility.
- Put opening braces on the same line as the declaration or condition.
- Use blank lines between logical blocks to keep code readable.
- Use concise comments to explain purpose, constraints, or non-obvious behavior.
- Choose appropriate numeric types and data structures.
- Avoid unnecessary allocations, clones, and conversions.
- Prefer borrowing when it keeps ownership straightforward.
- Handle expected failures explicitly. Avoid unwrap and expect on untrusted input.
- Follow the repository's formatter configuration.

## Project Structure

- Start with one root Cargo.toml and a conventional src/ directory.
- Add workspace crates only when there is a concrete need for separate packages.
- Keep lib.rs and main.rs small and focused on module declarations and startup.
- Put additional executable entry points in src/bin/.
- Organize code by domain and responsibility, using names that explain its purpose.
- Use mod.rs as the entry point for a directory of related modules.
- Keep transport concerns, such as HTTP routing and authentication, separate from core application logic.
- Let command-line tools, background jobs, and HTTP handlers reuse the same core logic.
- Keep small helpers close to their callers. Avoid catch-all helpers.rs or utils.rs files.
- Split files when they contain distinct responsibilities.
- Treat roughly 500 lines as a review signal, not a mandatory splitting threshold.
- Avoid creating a separate file for every small function or type.

## Types and Behavior

- Keep top-level structs, enums, type aliases, constants, and static data under src/types/.
- Group these declarations by domain, matching the organization of the application code.
- Avoid one giant types file or one file per trivial struct.
- Keep constructors, impl blocks, algorithms, and state transitions in the owning domain's logic modules.
- Re-export types through their owning domain.
- Preserve private field visibility. Do not expose internals just to make a file move easier.

## Changes and Verification

- Preserve existing behavior during structural refactoring.
- Be especially careful with serialization, numeric precision, ordering, concurrency, and public interfaces.
- Run the checks required by the repository and tests relevant to the change.
- Add focused tests for meaningful behavior and bug fixes.
- Avoid tests that merely repeat the implementation or confirm deleted code is absent.
- Do not keep broadening verification after relevant checks pass unless something remains unresolved.
- Report what changed, what was verified, and any remaining limitations.

## Collaboration

- Treat questions as requests for answers, not permission to edit files.
- When implementation is requested, carry it through to completion.
- Resolve routine implementation choices using the existing code and these preferences.
- Ask when missing information materially affects the result.
- Do not repeatedly request permission for actions already authorized.
- Use subagents only when independent work or additional review provides a clear benefit.
- When agents work in parallel, assign file ownership to prevent collisions.

## Git and Safety

- Check the working tree and current branch before changing anything.
- Preserve unrelated user changes.
- Use the branch agreed with the user and verify the repository's primary branch name.
- Do not create duplicate primary branches or rewrite shared history without explicit authorization.
- Be careful with destructive actions, including deleting files or branches.
- Never touch production, live databases, or shared deployment environments without explicit authorization.

## Pull Requests

- Follow the repository's title conventions.
- Explain the concrete problem, the resulting change, and relevant verification.
- Keep descriptions proportional to the change.
- Include a short attribution identifying the model and harness.
- Update the branch against the latest target branch before opening a PR.
- Open a regular PR unless a draft is explicitly requested.
- When asked to monitor a PR, verify review findings before acting.
- Fix real issues, explain false positives, and distinguish code failures from infrastructure failures.
- Stay quiet when there is no meaningful update.
- Do not use codex or claude in the name of PRs