{{- define "wait_postgres" }}
- name: wait-postgres
  image: {{ .Values.werf.image.alpine }}
  command: ['/bin/sh', '-c', 'while ! getent ahostsv4 {{ pluck .Values.werf.env (index .Values.app.database.host .Values.global.org .Values.global.app) | first | default (index .Values.app.database.host .Values.global.org .Values.global.app)._default }}; do sleep 1; done']
{{- end }}

{{- define "wait_redis" }}
- name: wait-redis
  image: {{ .Values.werf.image.alpine }}
  command: ['/bin/sh', '-c', 'while ! getent ahostsv4 {{ pluck .Values.werf.env (index .Values.app.redis.host .Values.global.org .Values.global.app) | first | default (index .Values.app.redis.host .Values.global.org .Values.global.app)._default }}; do sleep 1; done']
{{- end }}

{{- define "psql_check_db" }}
- name: check
  image: {{ .Values.werf.image.postgres }}
  imagePullPolicy: IfNotPresent
  command: ['/bin/sh', '-c', 'while ! PGPASSWORD="{{ pluck .Values.werf.env (index .Values.app.database.password .Values.global.org .Values.global.app) | first | default (index .Values.app.database.password .Values.global.org .Values.global.app)._default }}" psql -h {{ pluck .Values.werf.env (index .Values.app.database.host .Values.global.org .Values.global.app) | first | default (index .Values.app.database.host .Values.global.org .Values.global.app)._default }} -U {{ pluck .Values.werf.env (index .Values.app.database.user .Values.global.org .Values.global.app) | first | default (index .Values.app.database.user .Values.global.org .Values.global.app)._default }} -c "\c {{ pluck .Values.werf.env (index .Values.app.database.db .Values.global.org .Values.global.app) | first | default (index .Values.app.database.db .Values.global.org .Values.global.app)._default }}"; do sleep 3; done']
{{- end }}
