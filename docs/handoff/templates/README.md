# Repository Templates

These files are intended to be copied into the real Agent OS Go repository during Phase 0.

Required setup after `go mod init`:

```bash
go get -tool github.com/dominicnunez/archguard/cmd/archguard@latest
go get -tool github.com/dominicnunez/gallow/cmd/gallow@latest
go mod tidy
```

Then:

1. copy `.archguard.yaml` and set the actual `packages.root`;
2. copy `.gallowrc.json`;
3. copy `.githooks/` and ensure executable bits are retained;
4. copy `.github/workflows/ci.yml`;
5. configure `git config core.hooksPath .githooks`;
6. commit `go.mod`, `go.sum`, configs, hooks, and workflow.

After versions are intentionally selected/tagged, keep them pinned through the committed module graph rather than reinstalling `@latest` on every machine.
