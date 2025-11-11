#!/bin/bash

# COVID-19 Dashboard Docker Build and Push Script
# This script builds the Streamlit dashboard and pushes it to Google Artifact Registry

set -e

# Configuration
PROJECT_ID="skilful-union-474420-c7"
REGION="us-central1"
REPO="covid-docker-repo"
IMAGE_URI="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/covid-dashboard:v1"

echo "=== Building and Pushing COVID Dashboard Docker Image ==="
echo "Project ID: $PROJECT_ID"
echo "Image URI: $IMAGE_URI"

# Create Artifact Registry repository if it doesn't exist
echo "Creating Artifact Registry repository..."
gcloud artifacts repositories create $REPO \
    --repository-format=docker \
    --location=$REGION \
    --description="COVID Dashboard Docker Repository" \
    --project=$PROJECT_ID || echo "Repository may already exist"

# Configure Docker authentication
echo "Configuring Docker authentication..."
gcloud auth configure-docker $REGION-docker.pkg.dev

# Build and push the image
echo "Building and pushing Docker image..."
cd streamlit_dashboard
gcloud builds submit --tag $IMAGE_URI --project=$PROJECT_ID

echo "=== Docker image build and push completed successfully ==="
echo "Image available at: $IMAGE_URI"
