# Phase 2 Deployment Guide - Streamlit Dashboard on GKE

This guide provides step-by-step instructions for deploying the Streamlit dashboard to Google Kubernetes Engine after the Gold layer is ready in BigQuery.

## Prerequisites

- Phase 1 pipeline completed with data in `medallion_gold.covid_state_monthly`
- gcloud CLI configured with proper project access
- Docker installed (for local testing, optional)
- kubectl installed for Kubernetes management

## Step 1: Enable Required APIs

```bash
# Enable additional APIs for Phase 2
gcloud services enable artifactregistry.googleapis.com
gcloud services enable container.googleapis.com

# Verify all required APIs are enabled
gcloud services list --enabled | grep -E "(bigquery|storage|artifactregistry|container)"
```

## Step 2: Build and Push Docker Image

### Option A: Using Cloud Build (Recommended)

```bash
# Navigate to project directory
cd ~/covid_dashboard

# Create Artifact Registry repository
PROJECT_ID="skilful-union-474420-c7"
REGION="us-central1"
REPO="covid-docker-repo"

gcloud artifacts repositories create $REPO \
    --repository-format=docker \
    --location=$REGION \
    --description="COVID Dashboard Docker Repository"

# Configure Docker authentication
gcloud auth configure-docker $REGION-docker.pkg.dev

# Build and push using Cloud Build
IMAGE_URI="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/covid-dashboard:v1"
gcloud builds submit --tag $IMAGE_URI streamlit_dashboard/
```

### Option B: Using Automated Script

```bash
# Make script executable and run
chmod +x scripts/build-and-push.sh
./scripts/build-and-push.sh
```

## Step 3: Create GKE Cluster

```bash
# Create Autopilot cluster (recommended for simplicity)
gcloud container clusters create-auto covid-dashboard-cluster \
    --region us-central1 \
    --project skilful-union-474420-c7

# Get cluster credentials
gcloud container clusters get-credentials covid-dashboard-cluster \
    --region us-central1 \
    --project skilful-union-474420-c7

# Verify cluster connection
kubectl get nodes
```

## Step 4: Setup IAM and Workload Identity

### Create Google Service Account

```bash
# Create service account for dashboard
gcloud iam service-accounts create covid-dashboard-sa \
    --display-name="COVID Dashboard Service Account" \
    --project=skilful-union-474420-c7

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding skilful-union-474420-c7 \
    --member="serviceAccount:covid-dashboard-sa@skilful-union-474420-c7.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding skilful-union-474420-c7 \
    --member="serviceAccount:covid-dashboard-sa@skilful-union-474420-c7.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"
```

### Configure Workload Identity

```bash
# Bind Google Service Account to Kubernetes Service Account
gcloud iam service-accounts add-iam-policy-binding \
    covid-dashboard-sa@skilful-union-474420-c7.iam.gserviceaccount.com \
    --member="serviceAccount:skilful-union-474420-c7.svc.id.goog[default/default]" \
    --role="roles/iam.workloadIdentityUser"

# Annotate Kubernetes service account
kubectl annotate serviceaccount default \
    iam.gke.io/gcp-service-account=covid-dashboard-sa@skilful-union-474420-c7.iam.gserviceaccount.com --overwrite
```

## Step 5: Deploy to Kubernetes

### Deploy Application

```bash
# Apply deployment manifest
kubectl apply -f k8s/covid-dashboard-deployment.yaml

# Apply service manifest  
kubectl apply -f k8s/covid-dashboard-service.yaml

# Wait for deployment to be ready
kubectl rollout status deployment/covid-dashboard

# Check deployment status
kubectl get deployments
kubectl get pods
kubectl get services
```

### Alternative: Using Automated Script

```bash
# Make script executable and run
chmod +x scripts/deploy-to-gke.sh
./scripts/deploy-to-gke.sh
```

## Step 6: Access the Dashboard

### Get External IP Address

```bash
# Wait for LoadBalancer to assign external IP (may take 2-3 minutes)
kubectl get services covid-dashboard-service --watch

# Once EXTERNAL-IP is available, access the dashboard
# Example output:
# NAME                      TYPE           CLUSTER-IP       EXTERNAL-IP     PORT(S)        AGE
# covid-dashboard-service   LoadBalancer   34.118.225.192   104.197.26.75   80:31155/TCP   3m
```

### Test Dashboard Access

```bash
# Get the external IP
EXTERNAL_IP=$(kubectl get services covid-dashboard-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Dashboard URL: http://$EXTERNAL_IP"

# Test connectivity
curl -I http://$EXTERNAL_IP
```

## Step 7: Verify Dashboard Functionality

1. **Open Browser**: Navigate to `http://EXTERNAL-IP`
2. **Check Data Display**: Verify COVID-19 data table shows current data
3. **Test Visualizations**: Confirm line chart displays trends correctly
4. **Verify Real-time Updates**: Data should refresh every 10 minutes

## Monitoring and Maintenance

### Check Application Health

```bash
# Monitor pod status
kubectl get pods -l app=covid-dashboard

# View application logs
kubectl logs deployment/covid-dashboard

# Check resource usage
kubectl top pods
```

### Scaling (if needed)

```bash
# Scale deployment replicas
kubectl scale deployment covid-dashboard --replicas=2

# Verify scaling
kubectl get deployments covid-dashboard
```

### Update Dashboard

```bash
# Build new image version
gcloud builds submit --tag us-central1-docker.pkg.dev/skilful-union-474420-c7/covid-docker-repo/covid-dashboard:v2 streamlit_dashboard/

# Update deployment image
kubectl set image deployment/covid-dashboard covid-dashboard=us-central1-docker.pkg.dev/skilful-union-474420-c7/covid-docker-repo/covid-dashboard:v2

# Monitor rollout
kubectl rollout status deployment/covid-dashboard
```

## Troubleshooting

### Common Issues and Solutions

#### 1. BigQuery 403 Errors

**Problem**: Dashboard shows authentication errors
**Solution**: 
```bash
# Verify service account permissions
gcloud projects get-iam-policy skilful-union-474420-c7 \
    --flatten="bindings[].members" \
    --filter="bindings.members:covid-dashboard-sa@skilful-union-474420-c7.iam.gserviceaccount.com"

# Re-apply Workload Identity annotation
kubectl annotate serviceaccount default \
    iam.gke.io/gcp-service-account=covid-dashboard-sa@skilful-union-474420-c7.iam.gserviceaccount.com --overwrite
```

#### 2. Pod Stuck in Pending State

**Problem**: Pods not starting
**Solution**:
```bash
# Check pod events
kubectl describe pod <pod-name>

# Check cluster resources
kubectl top nodes
```

#### 3. Service External IP Pending

**Problem**: LoadBalancer not getting external IP
**Solution**:
```bash
# Check service events
kubectl describe service covid-dashboard-service

# Verify firewall rules
gcloud compute firewall-rules list --filter="name~gke"
```

#### 4. Dashboard Shows No Data

**Problem**: Empty dashboard or connection errors
**Solution**:
```bash
# Verify BigQuery table exists and has data
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`skilful-union-474420-c7.medallion_gold.covid_state_monthly\`"

# Check pod logs for specific errors
kubectl logs deployment/covid-dashboard
```

## Cleanup (Optional)

To remove all Phase 2 resources:

```bash
# Delete Kubernetes resources
kubectl delete service covid-dashboard-service
kubectl delete deployment covid-dashboard

# Delete GKE cluster
gcloud container clusters delete covid-dashboard-cluster --region=us-central1

# Delete Docker images
gcloud artifacts repositories delete covid-docker-repo --location=us-central1

# Delete service account
gcloud iam service-accounts delete covid-dashboard-sa@skilful-union-474420-c7.iam.gserviceaccount.com
```

## Success Verification

Your Phase 2 deployment is successful when:

- ✅ Docker image builds and pushes to Artifact Registry
- ✅ GKE cluster creates and accepts kubectl commands
- ✅ Workload Identity properly configured (no authentication errors)
- ✅ Dashboard pods running and healthy
- ✅ LoadBalancer service has external IP assigned
- ✅ Dashboard accessible via browser showing current COVID data
- ✅ Visualizations render correctly with interactive features

**Expected Result**: A fully functional web dashboard displaying real-time COVID-19 analytics from your BigQuery Gold layer, deployed on Google Kubernetes Engine with enterprise-grade security and scalability.
