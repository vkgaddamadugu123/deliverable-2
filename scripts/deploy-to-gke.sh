#!/bin/bash

# COVID-19 Dashboard GKE Deployment Script
# This script creates GKE cluster and deploys the Streamlit dashboard

set -e

# Configuration
PROJECT_ID="skilful-union-474420-c7"
REGION="us-central1"
CLUSTER_NAME="covid-dashboard-cluster"
GSA_NAME="covid-dashboard-sa"

echo "=== Deploying COVID Dashboard to GKE ==="

# Create GKE Autopilot cluster
echo "Creating GKE Autopilot cluster..."
gcloud container clusters create-auto $CLUSTER_NAME \
    --region $REGION \
    --project $PROJECT_ID || echo "Cluster may already exist"

# Get cluster credentials
echo "Getting cluster credentials..."
gcloud container clusters get-credentials $CLUSTER_NAME \
    --region $REGION \
    --project $PROJECT_ID

# Verify cluster connection
echo "Verifying cluster connection..."
kubectl get nodes

# Create Google Service Account
echo "Creating Google Service Account..."
gcloud iam service-accounts create $GSA_NAME \
    --display-name="COVID Dashboard Service Account" \
    --project=$PROJECT_ID || echo "Service account may already exist"

# Grant BigQuery permissions
echo "Granting BigQuery permissions..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# Setup Workload Identity
echo "Setting up Workload Identity..."
gcloud iam service-accounts add-iam-policy-binding \
    ${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
    --member="serviceAccount:${PROJECT_ID}.svc.id.goog[default/default]" \
    --role="roles/iam.workloadIdentityUser"

kubectl annotate serviceaccount default \
    iam.gke.io/gcp-service-account=${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com --overwrite

# Deploy application
echo "Deploying COVID dashboard..."
kubectl apply -f k8s/covid-dashboard-deployment.yaml
kubectl apply -f k8s/covid-dashboard-service.yaml

# Wait for deployment
echo "Waiting for deployment to be ready..."
kubectl rollout status deployment/covid-dashboard

# Get service details
echo "Getting service details..."
kubectl get services covid-dashboard-service

echo "=== Deployment completed successfully ==="
echo "Run 'kubectl get services' to get the external IP address"
