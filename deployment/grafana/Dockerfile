FROM grafana/grafana-oss

COPY grafana_dashboards_provider.yml /etc/grafana/provisioning/dashboards/local.yml
COPY clickhouse.yml /etc/grafana/provisioning/datasources/datasource.yaml

ADD dashboards /var/lib/grafana/dashboards

ENTRYPOINT [ "/run.sh" ]