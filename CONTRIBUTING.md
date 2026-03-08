# Contributing to Atleon

🤘🎉 First off, thanks for taking the time to contribute! 🎉🤘

This document should provide guidelines and pointers for contributing to this project:

 - [Do you have a question?](#question-do-you-have-a-question)
 - [Have you found a bug?](#beetle-have-you-found-a-bug)
 - [Have you written a patch that fixes a bug?](#wrench-have-you-written-a-patch-that-fixes-a-bug)
 - [Do you need a new Feature or want to modify an existing Feature?](#mag-do-you-need-a-new-feature-or-want-to-modify-an-existing-feature)
 - [Code Style](#art-code-style)
 - [PR and Commit Style](#sparkles-pr-and-commit-style)
 - [More details](#speech_balloon-more-details)

## ❓Do you have a question?

Our primary conduit for addressing questions is via [GitHub Discussions](https://github.com/orgs/atleon/discussions).

## 🪲 Have you found a bug?

Please use [GitHub Issues](https://github.com/atleon/atleon/issues) for bug reporting.

## 🔧 Have you written a patch that fixes a bug?

 - _Unless the fix is trivial_ (typo, cosmetic, doesn't modify behavior, etc.), there **should be an issue** associated with it. See [Have you found a bug?](#beetle-have-you-found-a-bug).
 - Wait for **Issue Triage** before you start coding.
 - Create a feature branch off of master with a meaningful name (ideally referencing issue you created)
 - Work on your change. Be sure to include **JUnit test cases**. This will greatly help maintainers while reviewing your change
 - **Run all tests locally** prior to submission: `./mvnw clean verify -U`
 - Finally, you are **ready to submit** your Pull-Request 🎉

## 🔍 Do you need a new feature or want to modify an existing feature?

 - Start a [discussion](https://github.com/orgs/atleon/discussions) with the maintainers about the feature
 - Proceed after receiving positive feedback
 - Follow the same process as [fixing a bug](#wrench-have-you-written-a-patch-that-fixes-a-bug)

## 🎨 Code style

Low-level coding style is based on [Palantir Java Format](https://github.com/palantir/palantir-java-format) and enforced via [Spotless](https://github.com/diffplug/spotless). Modified code can have formatting applied by invoking `./mvnw spotless:apply`. Higher level coding standards are mostly informed by Robert C. Martin's ["Clean Code: a Handbook of Agile Software Craftsmanship"](https://www.amazon.com/s?k=robert+martin+clean+code). Some of the important high-level coding standard TL;DRs:

1. Know, respect, and apply the [SOLID principles](https://en.wikipedia.org/wiki/SOLID)
2. If you're writing a comment, consider if the code could be made more expressive

## ✨ PR and commit style

- Strive for **descriptive commit messages**, as this helps during code review
- Once submitted, the review and discussion starts. If necessary, make adjustments in **further commits**
- _Once the PR is **approved**_ ✅, clean up and **prepare for merge**
- Please attempt to squash and rebase your commits before merging

### ✒️ Commit Message Convention

We use the following convention for commit message title and body:

```
[ISSUE_REF] (<primarily affected module>: )Short headline description of content
- Longer bulleted description entry #1 describing sub-content of the commit
- Longer bulleted description entry #2 describing sub-content of the commit
```

- `ISSUE_REF` should be a GitHub issue number (`#69`)
- `primarily affected module` should be the module that is most affected/targeted by the change. If there is no reasonably "primary" module, this can be omitted.
- Bulleted description entries may describe smaller sub-contents of the commit

Here are some example commit messages (note that the first line is the title, and the rest is the optional body):

```
[#493] Kafka: Indicate explicitly paused partitions on Consumer proxy
- Expose explicitly granted partition pauses
- Clean up code around affected components
```

```
[#353] Configure Spotless with Palantir format and apply it
```

## 💬 More Details

If there is information missing here, feel free to [start a discussion](https://github.com/orgs/atleon/discussions)! Also check out the [GitHub Wiki](../../wiki).
