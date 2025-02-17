# Contributing Guidelines

We welcome patches and pull requests to improve LmdbJava.

**Before submitting a PR, please run `mvn clean verify`**.
This will run:

* Tests
* Initial Test Coverage
* Source Code Formatting
* License Header Management

`mvn clean verify` is also run by CI, but it's quicker and easier to run
before submitting.

### Releasing

GitHub Actions will perform an official release whenever a developer executes
`mvn release:clean release:prepare`.
