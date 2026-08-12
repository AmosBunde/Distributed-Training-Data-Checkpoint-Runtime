# Architecture Diagrams

Explorable, standalone HTML diagrams built with the archify toolchain — dark/light
themes, pan/zoom, search, guided views, and truthful in-viewer export. Open any HTML
file directly in a browser; no server or network needed. Each diagram also ships a
standalone SVG export for embedding in docs.

| Diagram | HTML (interactive) | SVG (static) | Source spec |
|---|---|---|---|
| System architecture — control plane, data plane, storage, observability | [system-architecture.html](system-architecture.html) | [svg](system-architecture.svg) | [spec](src/system.architecture.json) |
| Training-step data flow — shard bytes from storage to the training step | [training-step-dataflow.html](training-step-dataflow.html) | [svg](training-step-dataflow.svg) | [spec](src/training-step.dataflow.json) |
| Checkpoint atomic commit — barrier, upload, verify-then-publish sequence | [checkpoint-commit-sequence.html](checkpoint-commit-sequence.html) | [svg](checkpoint-commit-sequence.svg) | [spec](src/checkpoint-commit.sequence.json) |
| Worker & lease lifecycle — state machine incl. eviction/fencing recovery | [worker-lease-lifecycle.html](worker-lease-lifecycle.html) | [svg](worker-lease-lifecycle.svg) | [spec](src/worker-lease.lifecycle.json) |

All four validated at archify's `showcase` quality profile (9 artifact checks,
0 composition errors/warnings). To regenerate after editing a spec:

```bash
archify deliver <type> docs/diagrams/src/<spec>.json docs/diagrams/<name>.html --quality showcase
```

The decisions these diagrams illustrate are recorded in [`../adr/`](../adr/).
