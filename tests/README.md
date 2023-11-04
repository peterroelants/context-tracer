# Tests

Tests are run with [PyTest](https://docs.pytest.org/).

To run all unit tests run the following:
```
./test/run_tests.sh
```


## test directory

The `./tests/` directory follows the `./src/` directory but with every file and folder name having the suffix `_test` appended. This is because:
* It enables [PyTest test discovery](https://docs.pytest.org/en/latest/explanation/goodpractices.html#test-discovery)
* It does replicate the project directory without resulting in naming conflicts.
* It keeps the `tests` directory separated from the `src` directory so tests don't have to be shipped in the resulting Python package.
