{{- define "volumes" }}
- name: config
  configMap:
    name: {{ .Chart.Name }}-config
- name: internal
  secret:
    secretName: secrets-internal
{{- range $tenant := .Values.app.tenants }}
{{- if $tenant.key.public }}
- name: {{ $tenant.name | lower }}
  secret:
    secretName: secrets-{{ $tenant.name | lower }}
{{- end }}
{{- end }}
{{- end }}

{{- define "volumeMounts" }}
- name: config
  mountPath: /app/App.toml
  subPath: App.toml
- name: internal
  mountPath: {{ printf "/app/%s" (pluck .Values.werf.env .Values.app.id_token.key | first | default .Values.app.id_token.key._default) }}
  subPath: private_key
{{- range $tenant := .Values.app.tenants }}
{{- if $tenant.key.public }}
- name: {{ $tenant.name | lower }}
  mountPath: {{ printf "/app/%s" (pluck $.Values.werf.env $tenant.key.public | first | default $tenant.key.public._default) }}
  subPath: public_key
{{- end }}
{{- end }}
{{- end }}