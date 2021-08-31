# Contributing to Rhapsody

:+1::tada: First off, thanks for taking the time to contribute! :tada::+1:

This document should provide guidelines and pointers for contributing to this project:

 - [Do you have a question?](#question-do-you-have-a-question)
 - [Have you found a bug?](#beetle-have-you-found-a-bug)
 - [Have you written a patch that fixes a bug?](#wrench-have-you-written-a-patch-that-fixes-a-bug)
 - [Do you need a new Feature or want to modify an existing Feature?](#mag-do-you-need-a-new-feature-or-want-to-modify-an-existing-feature)
 - [Code Style](#art-code-style)
 - [PR and Commit Style](#sparkles-pr-and-commit-style)
 - [More details](#speech_balloon-more-details)

## :question: Do you have a question?

Our primary conduit for addressing questions is via Google Groups. Please reach out to our [Forum](TODO).

## :beetle: Have you found a bug?

Please open Issues here on GitHub for bug reporting. Make sure to include the "bug" tag.

## :wrench: Have you written a patch that fixes a bug?

 - _Unless the fix is trivial_ (typo, cosmetic, doesn't modify behavior, etc.), there **should be an issue** associated with it. See [Have you found a bug?](#beetle-have-you-found-a-bug).
 - Wait for **Issue Triage** before you start coding.
 - Create a feature branch off of master with a meaningful name (ideally referencing issue you created)
 - Work on your change. Be sure to include **JUnit test cases**. This will greatly help maintainers while reviewing your change
 - **Run all tests locally** prior to submission: `./mvnw clean verify -U`
 - Finally, you're **ready to submit** your Pull-Request :+1:

## :mag: Do you need a new Feature or want to modify an existing Feature?

 - Start a discussion with the maintainers about the Feature
 - Proceed after receiving positive feedback
 - Follow the same process as [fixing a bug](#wrench-have-you-written-a-patch-that-fixes-a-bug)

## :art: Code style

Our code style is mostly based on Robert C. Martin's ["Clean Code: a Handbook of Agile Software Craftsmanship"](https://www.amazon.com/s?k=robert+martin+clean+code)

Some of the important TL;DRs:
1. Spaces, not tabs
1. Unix (LF), not DOS (CRLF) line endings
1. Eliminate all trailing whitespace
1. If you're writing a comment, consider if the code could be made more expressive
1. Preserve existing formatting; i.e. do not reformat code for its own sake
1. When in doubt, try to find existing examples of what you're trying to format

## :sparkles: PR and commit style

 - **Commit early and commit often**. Still strive for **descriptive commit messages** though, as this helps during the review
 - Once submitted, the review and discussion starts. If necessary, make adjustments in **further commits**
 - Use your **real name** in commits
 - _Once the PR is **approved**_ :white_check_mark:, clean up and **prepare for merge**
 - Please attempt to squash and merge your commits before merging

### :black_nib: Commit Message Convention

We use the following convention for commit message title and body:

```
[ISSUE_REF]Short headline description of content

Longer description of the content of the commit and/or context of the change
```

 - `ISSUE_REF` should be a GitHub issue number (`#69`)

## :speech_balloon: More Details

If there is information missing here, feel free to reach out! Also check out the [GitHub Wiki](../../wiki)
