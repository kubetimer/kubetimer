#!/bin/bash
# Deploy KubeTimer operator to Kubernetes cluster
# Usage: ./deploy.sh

set -e

echo "=== Deploying KubeTimer Operator ==="

# 1. Create namespace
echo "Creating namespace..."
kubectl apply -f deploy/namespace.yaml

# 2. Install CRD
echo "Installing CRD..."
kubectl apply -f deploy/crd.yaml

# 3. Setup RBAC
echo "Setting up RBAC..."
kubectl apply -f deploy/rbac.yaml

# 4. Deploy operator
echo "Deploying operator..."
kubectl apply -f deploy/deployment.yaml

# 5. Wait for operator to be ready
echo "Waiting for operator to be ready..."
kubectl -n kubetimer-system rollout status deployment/kubetimer-operator

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "To create a config, run:"
echo "  kubectl apply -f kubetimer/crd/example-config.yaml"
echo ""
echo "To check operator logs:"
echo "  kubectl -n kubetimer-system logs -l app=kubetimer-operator -f"
