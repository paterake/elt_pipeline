# JVM Toolchain Setup (macOS)

This guide sets up the Java runtime that Apache Spark requires, using a version manager that keeps the OS clean and stays out of the way of `uv` (which owns Python).

Platform documented here: **macOS (Apple Silicon)**. The `normalize`, `sql`, and `publish` stages run on Spark and will not start without a JVM — see the prerequisite note in [LOCAL_OPERATOR_RUNBOOK.md](../operator/LOCAL_OPERATOR_RUNBOOK.md).

## Why a JVM is required at all

PySpark is not a Python reimplementation of Spark — it is a thin Python wrapper over Spark's engine, which is written in Scala/Java and runs on the JVM. Your Python process (managed by `uv`) issues commands across a Py4J bridge to a JVM process that does all query planning, execution, shuffles, and I/O. **The JVM is Spark; Python is how you talk to it.** No JVM ⇒ Spark cannot start.

## Version requirement

| Component | Version | Source |
| --- | --- | --- |
| PySpark | `4.1.2` | pinned in [pyproject.toml](../../pyproject.toml) |
| JDK | **Java 17** (Temurin) | required by Spark 4.x (supports 17 or 21; 17 is the default here) |

Spark discovers the JVM through the `JAVA_HOME` environment variable. If Spark ever reports "Unable to locate a Java Runtime" or a `JAVA_HOME` error, that variable is almost always the cause.

## Tool choice: `mise` (and why)

We manage the JDK with [`mise`](https://mise.jdx.dev/), installed via Homebrew. Rationale, given a hard requirement to keep the OS as clean as possible and to not collide with `uv`:

- **Minimal OS footprint** — a single Homebrew binary. Everything else (the JDK itself) lives under `~/.local/share/mise/`. No `sudo`, nothing in `/usr/bin` or `/Library`.
- **Clean uninstall** — `brew uninstall mise` plus `rm -rf ~/.local/share/mise`.
- **Sets `JAVA_HOME` automatically** on shell activation — no manual exports.
- **Stays in its lane** — used for Java only. `uv` remains the sole owner of Python; do **not** run `mise use python`.

### Why not SDKMAN!

SDKMAN! is the JVM-world standard and equally clean *once installed*, but its installer requires Bash 4+, and macOS ships Bash 3.2. Bootstrapping it forces `brew install bash` — installing an OS package solely to run an installer, which is exactly the pollution we are avoiding. `mise` needs no such step, so it wins on the "keep the OS pure" axis.

### Absolute-minimum alternative (not used)

The theoretically purest option is no manager at all: download the Temurin 17 `.tar.gz`, extract into `~/`, and point `JAVA_HOME` at it — zero OS package footprint. Rejected because it loses clean version switching and repo pinning for negligible additional purity over `mise`.

## Setup steps

These are the exact steps used to provision this workstation.

```bash
# 1. Install the version manager (one Homebrew binary)
brew install mise

# 2. Activate it for zsh (adds one line to ~/.zshrc), then restart the shell
echo 'eval "$(mise activate zsh)"' >> ~/.zshrc
exec zsh

# 3. Install Temurin 17 globally
mise use -g java@temurin-17

# 4. Verify the JDK and JAVA_HOME
java -version          # -> openjdk version "17.0.x" ... Temurin
echo "$JAVA_HOME"      # -> ~/.local/share/mise/installs/java/temurin-17.0.x
```

### Deliberately skipped: macOS system integration

After installing the JDK, `mise` prints an optional step that symlinks the JDK into `/Library/Java/JavaVirtualMachines/` via `sudo`. **Skip it.** It exists only for tools that scan the macOS system JDK location (e.g. `/usr/libexec/java_home`); Spark/PySpark read `JAVA_HOME` and do not need it. Running it would place symlinks in a system path — the OS pollution we are avoiding.

## Verifying PySpark boots the JVM

From the repository root, confirm the full path works end-to-end (mise supplies `JAVA_HOME`, `uv` supplies Python):

```bash
uv run python -c "
from pyspark.sql import SparkSession
s = SparkSession.builder.master('local[1]').appName('jvm-check').getOrCreate()
print('Spark version:', s.version)
print('row check:', s.range(3).count())
s.stop()
"
```

Expected output includes `Spark version: 4.1.2` and `row check: 3`, with no `JAVA_HOME` / "Unable to locate a Java Runtime" errors. This exact check was run during provisioning and passed against Temurin 17.0.20 + PySpark 4.1.2.

## Optional: pin the JDK to the repository

To make the Java version explicit for teammates, CI, and other tools (the `uv`/`.python-version` equivalent), run **in the repo root**:

```bash
mise use java@temurin-17     # writes ./mise.toml, auto-switches on cd into the repo
```

Commit the resulting `mise.toml` so everyone resolves to the same JDK. Until then, the global install from step 3 covers local development.

## Troubleshooting

| Symptom | Cause / Fix |
| --- | --- |
| `Unable to locate a Java Runtime` | `JAVA_HOME` unset in this shell. Confirm `mise activate zsh` is in `~/.zshrc` and the shell was restarted; check `echo "$JAVA_HOME"`. |
| `java -version` shows 3.2 / system Java | mise not activated for the current shell, or an older Java earlier on `PATH`. Run `mise doctor` and `which -a java`. |
| Wrong Java version picked up | A repo-local `mise.toml` or a different global pin. Check `mise ls java` and `mise current`. |
| Spark starts but stage fails on JVM version | Spark 4.x needs Java 17 or 21. Ensure the active JDK is 17 (`java -version`). |

## Provenance

Established 2026-08-15 while making the pipeline all-Spark. Verified: `mise` 2026.8.6, Temurin 17.0.20, PySpark 4.1.2, macOS (Apple Silicon), zsh. No system-level Java or `sudo` integration installed.
