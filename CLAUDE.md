# Working on taskmill

taskmill is a library that other teams run in production. Hold every change,
however small, to these goals:

- **Correct and safe.** Think through failures, concurrency, and hostile
  input before the happy path. Never lose or duplicate a task, never leak
  data, never trust input.
- **Reliable.** When the database, the network, or a worker is slow or gone,
  degrade gracefully, say so, and recover without help.
- **Efficient and light.** Bound every query, request, and loop. Be kind to
  the database, slow networks, and old machines. Add no dependency without an
  ADR (see [docs/adr](docs/adr)).
- **Well designed and compact.** Choose the design a senior maintainer would
  be proud of, then build its smallest complete form: clear names, plain
  words, no dead code, no speculative options. When a trade-off is real,
  decide deliberately and say why.
- **Complete.** Tests at the real boundary, docs and the changelog in the
  same change, `make check` passing, and the result seen working: run it,
  and check UI changes in a browser, in both themes and at phone width.

[CONTRIBUTING.md](CONTRIBUTING.md) covers setup, commits, and pull requests.
