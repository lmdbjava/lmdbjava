= Contributing Guidelines

We welcome patches and pull requests to improve LmdbJava.

For small changes, please simply submit a
[pull request](https://github.com/lmdbjava/lmdbjava/pulls).

For larger changes, please
[open a GitHub issue](https://github.com/lmdbjava/lmdbjava/issues) first so that
we can discuss what you have in mind.

All engineering decisions require trade-offs, which is why we have an ordered list of 
[priorites](https://github.com/lmdbjava/lmdbjava/blob/master/src/main/java/org/lmdbjava/package-info.java).
Please ensure your changes reflect those priorities.

In terms of style, use the current code as your guide. Highlights:

* Add the copyright header to each file
* Use `final`
* Use `import static`
* Keep methods short and clear
* Exceptions extend `LmdbException` and are typically static inner classes
* JavaDocs are needed for public types and methods
* Test coverage is important (Travis and Coveralls run for every pull request)

If you have any questions, please
[open a GitHub issue](https://github.com/lmdbjava/lmdbjava/issues) and we'll be
pleased to help.

Thanks for your interest in contributing!
