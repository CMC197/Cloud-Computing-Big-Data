{{/*
  SmartPark — Standard-Helper.
  AP5 füllt templates/ Schritt für Schritt (Phase 6.1 der Checkliste):
  kafka-statefulset.yaml, minio-statefulset.yaml, spark-deployment.yaml,
  api-deployment.yaml, producer-deployment.yaml, ui-deployment.yaml,
  services.yaml, ingress.yaml, configmap.yaml, secret.yaml, hpa.yaml
*/}}

{{- define "smartpark.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "smartpark.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "smartpark.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels für alle Ressourcen */}}
{{- define "smartpark.labels" -}}
helm.sh/chart: {{ include "smartpark.chart" . }}
{{ include "smartpark.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "smartpark.selectorLabels" -}}
app.kubernetes.io/name: {{ include "smartpark.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
  Komponenten-Labels.
  Aufruf:  {{- include "smartpark.componentLabels" (dict "ctx" . "component" "api") | nindent 4 }}
*/}}
{{- define "smartpark.componentLabels" -}}
{{ include "smartpark.labels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "smartpark.componentSelectorLabels" -}}
{{ include "smartpark.selectorLabels" .ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
  Voll qualifizierter Image-Name.
  Aufruf:  {{ include "smartpark.image" (dict "ctx" . "image" .Values.api.image) }}
  Skaffold überschreibt image.repository/tag beim Deploy per setValues.
*/}}
{{- define "smartpark.image" -}}
{{- $registry := .ctx.Values.global.imageRegistry -}}
{{- $repo := .image.repository -}}
{{- $tag := default .ctx.Chart.AppVersion .image.tag -}}
{{- if contains "/" $repo -}}
{{- printf "%s:%s" $repo $tag -}}
{{- else -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- end -}}
{{- end -}}

{{/* Name der ConfigMap bzw. des Secrets */}}
{{- define "smartpark.configMapName" -}}
{{- printf "%s-config" (include "smartpark.fullname" .) -}}
{{- end -}}

{{- define "smartpark.secretName" -}}
{{- if .Values.minio.auth.existingSecret -}}
{{- .Values.minio.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-minio" (include "smartpark.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Kafka-Bootstrap-Adresse, an einer Stelle definiert */}}
{{- define "smartpark.kafkaBootstrap" -}}
{{- printf "%s-kafka:9092" (include "smartpark.fullname" .) -}}
{{- end -}}

{{/* S3-Endpoint für Spark und API */}}
{{- define "smartpark.s3Endpoint" -}}
{{- printf "http://%s-minio:9000" (include "smartpark.fullname" .) -}}
{{- end -}}
