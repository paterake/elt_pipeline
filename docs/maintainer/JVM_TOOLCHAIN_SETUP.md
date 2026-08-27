# JVM Toolchain Setup (macOS)

This guide sets up the Java runtime that Apache Spark and Trino require, using a version manager that keeps the OS clean and stays out of the way of `uv` (which owns Python).

Platform documented here: **macOS (Apple Silicon)**. The `normalize`, `sql`, and `publish` stages run on Spark and will not start without a JVM — see the prerequisite note in [LOCAL_OPERATOR_RUNBOOK.md](../operator/LOCAL_OPERATOR_RUNBOOK.md). The reference Trino serving endpoint also runs on the JVM and requires the same JDK major version.

## Why a JVM is required at all

PySpark is not a Python reimplementation of Spark — it is a thin Python wrapper over Spark's engine, which is written in Scala/Java and runs on the JVM. Your Python process (managed by `uv`) issues commands across a Py4J bridge to a JVM process that does all query planning, execution, shuffles, and I/O. **The JVM is Spark; Python is how you talk to it.** No JVM ⇒ Spark cannot start. The same JDK also boots the Trino 468 JDBC serving engine.

## Version requirement

| Component | Version | Source |
| --- | --- | --- |
| PySpark | `4.1.2` | pinned in [pyproject.toml](../../pyproject.toml) |
| JDK | **Java 23** (Temurin) | required by **Trino 468** (the platform's reference JDBC/BI serving engine). Spark 4.x supports 17 / 21 / 23, but Trino pins the combined stack to 23. |

Spark discovers the JDK through the `JAVA_HOME` environment variable. If Spark ever reports "Unable to locate a Java Runtime" or a `JAVA_HOME` error, that variable is almost always the cause.

### Why JDK 23 and not 17 or 21

Trino 468 requires JDK 23. Because the platform's design-point serving path (Trino JDBC → Iceberg catalog) runs side-by-side with Spark on the same workstation, the whole toolchain (Spark driver JVM, Spark executors, Trino server JVM) converges on a single Temurin 23 install so operators only manage one JDK. JDK 23 removed the `SecurityManager`; the pipeline compensates by injecting these flags into the Spark driver and executor `extraJavaOptions` and into Trino's `jvm.config`:

```
-Djava.security.manager=allow -Djdk.security.allowAllPermissions=true
```

See [spark/session.py](../../src/elt_pipeline/spark/session.py) for the Spark-side injection, and the Trino bootstrap in `ops/trino_serving/` for the Trino-side injection. CI validates on Temurin 23 (see `.github/workflows/ci.yml`).

## Tool choice: `mise` (and why)

We manage the JDK with [`mise`](https://mise.jdx.dev/), installed via Homebrew. Rationale, given a hard requirement to keep the OS as clean as possible and to not collide with `uv`:

- **Minimal OS footprint** — a single Homebrew binary. Everything else (the JDK itself) lives under `~/.local/share/mise/`. No `sudo`, nothing in `/usr/bin` or `/Library`.
- **Clean uninstall** — `brew uninstall mise` plus `rm -rf ~/.local/share/mise`.
- **Sets `JAVA_HOME` automatically** on shell activation — no manual exports.
- **Stays in its lane** — used for Java only. `uv` remains the sole owner of Python; do **not** run `mise use python`.

### Why not SDKMAN!

SDKMAN! is the JVM-world standard and equally clean *once installed*, but its installer requires Bash 4+, and macOS ships Bash 3.2. Bootstrapping it forces `brew install bash` — installing an OS package solely to run an installer, which is exactly the pollution we are avoiding. `mise` needs no such step, so it wins on the "keep the OS pure" axis.

### Absolute-minimum alternative (not used)

The theoretically purest option is no manager at all: download the Temurin 23 `.tar.gz`, extract into `~/`, and point `JAVA_HOME` at it — zero OS package footprint. Rejected because it loses clean version switching and repo pinning for negligible additional purity over `mise`.

## Setup steps

```bash
# 1. Install the version manager (one Homebrew binary)
brew install mise

# 2. Activate it for zsh (adds one line to ~/.zshrc), then restart the shell
echo 'eval "$(mise activate zsh)"' >> ~/.zshrc
exec zsh

# 3. Install Temurin 23 globally
mise use -g java@temurin-23

# 4. Verify the JDK and JAVA_HOME
java -version          # -> openjdk version "23"  ... Temurin-23.x+y
echo "$JAVA_HOME"      # -> ~/.local/share/mise/installs/java/temurin-23.x.y
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

Expected output includes `Spark version: 4.1.2` and `row check: 3`, with no `JAVA_HOME` / "Unable to locate a Java Runtime" errors and no `SecurityManager` deprecation crashes.

## Optional: pin the JDK to the repository

To make the Java version explicit for teammates, CI, and other tools (the `uv`/`.python-version` equivalent), run **in the repo root**:

```bash
mise use java@temurin-23     # writes ./mise.toml, auto-switches on cd into the repo
```

Commit the resulting `mise.toml` so everyone resolves to the same JDK. Until then, the global install from step 3 covers local development.

## Troubleshooting

| Symptom | Cause / Fix |
| --- | --- |
| `Unable to locate a Java Runtime` | `JAVA_HOME` unset in this shell. Confirm `mise activate zsh` is in `~/.zshrc` and the shell was restarted; check `echo "$JAVA_HOME"`. |
| `java -version` shows 3.2 / system Java | mise not activated for the current shell, or an older Java earlier on `PATH`. Run `mise doctor` and `which -a java`. |
| Wrong Java version picked up | A repo-local `mise.toml` or a different global pin. Check `mise ls java` and `mise current`. |
| Spark or Trino fails with `SecurityManager` / policy error | You are on JDK 23 but the startup flags are missing. Spark: confirm `spark.driver.extraJavaOptions` contains `-Djava.security.manager=allow -Djdk.security.allowAllPermissions=true` (injected by the session builder). Trino: confirm the same two flags are in `jvm.config`. |
| `test_maintenance` sandbox exits with `JAVA_GATEWAY_EXITED` / Spark OOM on 4-core machines | The default 1g driver heap is too small for the 8-way concurrent maintenance sandbox run (each Spark subprocess holds its own JVM). Fix by exporting the tuning knob before the gate: `export ELT_PIPELINE_TEST_MAINTENANCE_JVM_MEM=2g`. CI uses 2g on all runners as the baseline. |
