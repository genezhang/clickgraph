{{/*
Expand the name of the chart.
*/}}
{{- define "clickgraph.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "clickgraph.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "clickgraph.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "clickgraph.labels" -}}
helm.sh/chart: {{ include "clickgraph.chart" . }}
{{ include "clickgraph.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "clickgraph.selectorLabels" -}}
app.kubernetes.io/name: {{ include "clickgraph.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "clickgraph.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "clickgraph.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
ClickHouse URL
*/}}
{{- define "clickgraph.clickhouseUrl" -}}
{{- if .Values.clickhouse.enabled }}
{{- printf "http://%s-clickhouse:8123" .Release.Name }}
{{- else if .Values.clickhouse.external.enabled }}
{{- printf "http://%s:%d" .Values.clickhouse.external.host (.Values.clickhouse.external.port | int) }}
{{- end }}
{{- end }}
