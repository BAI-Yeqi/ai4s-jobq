# Image SHA denylist

The image-SHA denylist lets operators stop and prevent jobq workers from
running a specific container image, identified by **either** its multi-arch
manifest digest **or** an arch-specific child digest (`sha256:…`). When an
image is compromised or broken, add its digest to the denylist and the
workforce will cancel matching workers, refuse to launch new ones, and each
worker's own periodic self-check will shut it down.

## How it works

- **Centralized store.** The denylist is a shared Azure Table. A single
  org-wide account can serve every workforce, or each deployment can point
  at its own. Entries are keyed by the normalized image digest.
- **Workforce enforcement.** Each autoscale tick the workforce reads the
  denylist, cancels every active or pending worker whose recorded image
  digest is denied, and skips hiring for a denied prototype image.
- **Worker self-check.** At hire time the workforce records each worker's
  resolved digests as the `JOBQ_IMAGE_DIGEST` (manifest/list digest) and
  `JOBQ_IMAGE_DIGEST_ARCH` (linux/amd64 child digest) environment variables.
  A background task polls the denylist and shuts the worker down when it
  finds its own image, using the shutdown severity recorded on the entry.

Per-worker matching requires the digest to be known at hire time, so an
`ImageDigestResolver` must be active. Workers hired without a resolved
digest carry no `JOBQ_IMAGE_DIGEST` and cannot be denied.

## Configuration

All configuration is via environment variables read by both the workforce
and the workers.

| Variable | Default | Purpose |
| --- | --- | --- |
| `JOBQ_DENYLIST_ACCOUNT` | `jobq0central` | Storage account name, connection string, or Azurite `devstoreaccount1`. Defaults to the shared org-wide account; override to point at your own. |
| `JOBQ_DENYLIST_TABLE` | `JobQImageDenylist` | Table name holding the entries. |
| `JOBQ_DENYLIST_DISABLE` | _unset_ | Hard off switch for the whole feature. |
| `JOBQ_DENYLIST_POLL_INTERVAL_S` | `60` | Worker self-check cadence, in seconds. |
| `JOBQ_DENYLIST_REQUIRE` | auto | Force fail-closed behavior. Auto-enabled on managed compute (for example Singularity); set to `0` to opt out. |

When the store is unreachable the denylist is **fail-open**: a missing or
unreachable store never cancels a healthy worker or blocks hiring. The
default account (`jobq0central`) is the shared org-wide store; override
`JOBQ_DENYLIST_ACCOUNT` to point at your own, or set `JOBQ_DENYLIST_DISABLE=1`
to turn the feature off entirely.

### Provisioning and permissions

Provision the configured table before enabling required mode. A missing table
is treated as an unavailable denylist, not as an empty denylist.

- Schedulers and workers need the **Storage Table Data Reader** role.
- Operators that add, update, or remove entries need the
  **Storage Table Data Contributor** role.

The client attempts to create the table when its identity has permission. A
runtime identity with read-only access cannot create it, so deployments should
provision the table once with an operator identity before launching workers.

### Fail-closed hardening

On managed compute where an undeniable worker is unacceptable (detected via
Singularity/AzureML environment markers), the denylist becomes mandatory:

- The worker **refuses to start** unless a denylist account is configured
  and the store is reachable at startup.
- A running worker that **cannot reach** the store for a sustained window
  shuts itself down rather than continuing unmonitored.

Set `JOBQ_DENYLIST_REQUIRE=0` to opt out, or `JOBQ_DENYLIST_DISABLE=1` to
turn the feature off entirely.

## CLI

Manage entries with the `ai4s-jobq denylist` command group. It accepts a
digest in `sha256:<hex>`, bare 64-hex, or `ref@sha256:<hex>` form.

```bash
# Add a denied digest (graceful shutdown by default). The audit "added by"
# field is auto-derived from your Azure token (UPN, else object id).
ai4s-jobq denylist add sha256:<hex> --reason "CVE-1234"

# Override the derived identity explicitly
ai4s-jobq denylist add sha256:<hex> --reason "CVE-1234" --added-by alice

# Deny with an immediate hard stop
ai4s-jobq denylist add sha256:<hex> --shutdown-mode hard

# Schedule a deny to take effect later (until then it is listed but not
# enforced). Accepts an ISO date/datetime, or "now" (the default).
ai4s-jobq denylist add sha256:<hex> --reason "CVE-1234" --effective 2026-07-29

# Overwrite an existing entry (adding a duplicate fails without --force)
ai4s-jobq denylist add sha256:<hex> --reason "updated" --force

# Inspect the denylist
ai4s-jobq denylist list
ai4s-jobq denylist list --as-json

# Check a single digest (exit code 1 when denied)
ai4s-jobq denylist check sha256:<hex>

# Remove an entry
ai4s-jobq denylist remove sha256:<hex>
```

`--added-by` records who added an entry for auditing. When omitted it is
auto-derived from the caller's Azure AD token (`upn`, falling back to the
object id); it stays blank when the store uses connection-string auth.
Entries stay in effect until an operator removes them with
`ai4s-jobq denylist remove`.

`denylist add` refuses to overwrite an existing entry for the same digest: if
the digest is already denied it fails, so one operator does not silently clobber
an entry that someone else placed (for example, one with an immediate hard
stop). Pass `--force` to overwrite an existing entry deliberately.

`--effective` schedules when a deny takes effect. It defaults to `now`
(effective immediately). An entry whose effective date is still in the future
is stored and shown by `denylist list` (marked `(pending)`), but it is **not**
enforced yet: matching workers keep running and hires are not refused until the
effective date arrives. This lets you stage a deny ahead of a hard deadline—for
example a week before a remediation due date—while giving teams time to migrate.

### Shutdown modes

Each entry records how a matching running worker should stop:

- `graceful` (default): the worker finishes its current task and takes no
  new work before exiting.
- `hard`: the worker's running task is cancelled and it exits promptly.
