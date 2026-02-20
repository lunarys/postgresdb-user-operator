# postgresdb-user-operator

A Kubernetes operator that provisions PostgreSQL databases and users via [CloudNativePG](https://cloudnative-pg.io/). Declare a `PostgresDatabase` resource and the operator creates the database, a dedicated user, and a connection-credentials Secret.

I created this operator because I use CloudNativePG for my homelab cluster, 
and spinning up a separate CNPG cluster for every small use case is overkill. 
Instead, this operator provisions isolated databases and users within a single shared cluster.

## Prerequisites

- Kubernetes v1.24+
- [CloudNativePG](https://cloudnative-pg.io/) installed with at least one `Cluster` resource

## How it works

The operator connects to the CNPG cluster using its `<cluster-name>-superuser` Secret (created by CNPG) and the read-write service endpoint (`<cluster-name>-rw.<namespace>.svc.cluster.local:5432`). It then creates the database and user via SQL, and writes a credentials Secret whose keys match the format CNPG uses for its own app user secrets (`username`, `password`, `host`, `port`, `dbname`, `uri`, `jdbc-uri`, `pgpass`).

## Installation

### Helm (recommended)

```sh
helm install postgresdb-user-operator \
  oci://ghcr.io/lunarys/charts/postgresdb-user-operator \
  --version <version> \
  --namespace postgresdb-user-operator-system \
  --create-namespace \
  --set defaultCluster.selector=environment=prod
```

Key values:

| Value | Default | Description |
|-------|---------|-------------|
| `defaultCluster.selector` | `""` | Label selector to find the default CNPG cluster |
| `defaultCluster.name` | `""` | Explicit default cluster name (mutually exclusive with `selector`) |
| `defaultCluster.namespace` | `""` | Namespace scope for the selector search |
| `controllerManager.manager.image.tag` | `""` | Image tag; defaults to the chart's `appVersion` |
| `controllerManager.replicas` | `1` | Number of operator replicas |

### kubectl / kustomize

```sh
make install   # install CRDs
make deploy IMG=ghcr.io/lunarys/postgresdb-user-operator:<tag>
```

## Usage

Create a `PostgresDatabase` resource:

```yaml
apiVersion: postgres.crds.lunarys.lab/v1alpha1
kind: PostgresDatabase
metadata:
  name: myapp
  namespace: default
spec:
  # omit clusterRef to use the operator's configured default cluster
  clusterRef:
    name: my-pg-cluster
    namespace: databases
  databaseName: myapp_db   # optional; defaults to CR name with hyphens replaced by underscores
  username: myapp_user     # optional; defaults to databaseName
  secretName: myapp-pgcreds  # optional; defaults to <cr-name>-pgcreds
  deletionPolicy: Retain   # Retain (default) or Delete
```

The operator creates:
- The PostgreSQL database on the target CNPG cluster
- A dedicated PostgreSQL user as the database owner
- A Kubernetes Secret with keys matching CNPG's app-user secret format:
  `username`, `password`, `host`, `port`, `dbname`, `uri`, `jdbc-uri`, `pgpass`

Status is reflected via conditions (`Ready=True/False`) and printer columns visible in `kubectl get pgdb`.

## Development

```sh
make test           # run unit tests
make helm-generate  # regenerate chart/ from kustomize + apply patches
make build          # build the manager binary
```

Run `make help` for all available targets.

## License

Copyright 2026. Licensed under the [Apache License, Version 2.0](LICENSE).
