# promagg 0.2.2: binaries

Built from tag `v0.2.2`. The code is on `main`; this branch only holds the files to install.
Each version has its own branch `binaries-<version>`; the branches of older versions are kept.

| file | what |
|---|---|
| `promagg-0.2.2/promagg-0.2.2-py3-none-any.whl` | the promagg wheel (pure Python). Its dependencies (duckdb) are in the osagg offline zip (repo osagg), which also contains this wheel |
| `promagg-0.2.2/SHA256SUMS` | checksums |

Download: open the file on GitHub, then **Download raw file**; check it with `sha256sum -c SHA256SUMS`.
Install into Superset's virtualenv: `pip install promagg-0.2.2-py3-none-any.whl` (see the README on `main`).
