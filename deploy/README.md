# ============================================================================
# elt_pipeline — reference Kubernetes manifests
# ----------------------------------------------------------------------------
# These are reference /templates/, not "turnkey production manifests. Use as a starting point
# and adapt to your cluster:
#   - StorageClass: the PVCs request 2 volumes (shared readwriteonce w/ standard; swap for
#     RWX NFS/EFS if you need multi-attached warehouse access across pods.
#   - Resources: set requests/limits sized for a 4-core / 16GB demo; size up for prod.
#   - Catalog binding: the ConfigMap below pins serving_catalog_type=jdbc (zero-service
#     sqlite metastore) which only works with a single writer + single reader.
#     For multi-replica Trino serving, switch to catalog_type=rest (Polaris/
#     Nessie/Lakekeeper) or catalog_type=glue and pass the matching config.
# ============================================================================
#
# Usage:
#   kubectl apply -k deploy/overlays/dev
#
# Structure:
#   base/
#     configmap.yaml      — pipeline.yaml mounted at /etc/elt_pipeline/pipeline.yaml
#     pvc-warehouse.yaml  — /var/lib/elt_pipeline (shared Iceberg warehouse)
#     service-trino.yaml      — ClusterIP for Trino serving (port 8080)
#     deployment-trino.yaml
#     cronjob-eltdaily-elt.yaml
#   overlays/dev/
#     kustomization.yaml
#     namespace.yaml
# ============================================================================
