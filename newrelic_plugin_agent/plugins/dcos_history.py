"""
dcos_history
"""

import logging
import re

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class DcosHistory(base.JSONStatsPlugin):
    def parse(self, data):
        assert data and data != "", "None response data"
        cluster_name = data['cluster']
        cluster_hostname = data['hostname']
        slaves = data['slaves']
        frameworks = data['frameworks']

        dcos_metrics = {}
        dcos_metrics[cluster_name] = {}
        dcos_metrics[cluster_name]['tasks'] = {}
        dcos_metrics[cluster_name]['tasks']['ERROR'] = 0
        dcos_metrics[cluster_name]['tasks']['FAILED'] = 0
        dcos_metrics[cluster_name]['tasks']['KILLED'] = 0
        dcos_metrics[cluster_name]['tasks']['FINISHED'] = 0
        dcos_metrics[cluster_name]['tasks']['LOST'] = 0
        dcos_metrics[cluster_name]['tasks']['RUNNING'] = 0
        dcos_metrics[cluster_name]['tasks']['STAGING'] = 0
        dcos_metrics[cluster_name]['tasks']['STARTING'] = 0
        dcos_metrics[cluster_name]['offered_resources'] = {}
        dcos_metrics[cluster_name]['offered_resources']['cpus'] = 0
        dcos_metrics[cluster_name]['offered_resources']['mem'] = 0
        dcos_metrics[cluster_name]['offered_resources']['disk'] = 0
        dcos_metrics[cluster_name]['resources'] = {}
        dcos_metrics[cluster_name]['resources']['cpus'] = 0
        dcos_metrics[cluster_name]['resources']['mem'] = 0
        dcos_metrics[cluster_name]['resources']['disk'] = 0
        dcos_metrics[cluster_name]['used_resources'] = {}
        dcos_metrics[cluster_name]['used_resources']['cpus'] = 0
        dcos_metrics[cluster_name]['used_resources']['mem'] = 0
        dcos_metrics[cluster_name]['used_resources']['disk'] = 0

        slaves_metrics = {}

        for fm in frameworks:
            dcos_metrics[cluster_name]['tasks']['ERROR'] += fm['TASK_ERROR']
            dcos_metrics[cluster_name]['tasks']['FAILED'] += fm['TASK_FAILED']
            dcos_metrics[cluster_name]['tasks']['KILLED'] += fm['TASK_KILLED']
            dcos_metrics[cluster_name]['tasks']['FINISHED'] += fm['TASK_FINISHED']
            dcos_metrics[cluster_name]['tasks']['LOST'] += fm['TASK_LOST']
            dcos_metrics[cluster_name]['tasks']['RUNNING'] += fm['TASK_RUNNING']
            dcos_metrics[cluster_name]['tasks']['STAGING'] += fm['TASK_STAGING']
            dcos_metrics[cluster_name]['tasks']['STARTING'] += fm['TASK_STARTING']
            dcos_metrics[cluster_name]['offered_resources']['cpus'] += fm['offered_resources']['cpus']
            dcos_metrics[cluster_name]['offered_resources']['mem'] += fm['offered_resources']['mem'] * 1024 * 1024
            dcos_metrics[cluster_name]['offered_resources']['disk'] += fm['offered_resources']['disk'] * 1024 * 1024
            dcos_metrics[cluster_name]['used_resources']['cpus'] += fm['used_resources']['cpus']
            dcos_metrics[cluster_name]['used_resources']['mem'] += fm['used_resources']['mem'] * 1024 * 1024
            dcos_metrics[cluster_name]['used_resources']['disk'] += fm['used_resources']['disk'] * 1024 * 1024
            for slave_id in fm['slave_ids']:
                slaves_metrics[slave_id] = {}
                slaves_metrics[slave_id]['tasks'] = {}
                slaves_metrics[slave_id]['offered_resources'] = {}
                slaves_metrics[slave_id]['resources'] = {}
                slaves_metrics[slave_id]['used_resources'] = {}

        for slave in slaves:
            slaves_metrics[slave['id']]['tasks']['ERROR'] = slave['TASK_ERROR']
            slaves_metrics[slave['id']]['tasks']['FAILED'] = slave['TASK_FAILED']
            slaves_metrics[slave['id']]['tasks']['KILLED'] = slave['TASK_KILLED']
            slaves_metrics[slave['id']]['tasks']['FINISHED'] = slave['TASK_FINISHED']
            slaves_metrics[slave['id']]['tasks']['LOST'] = slave['TASK_LOST']
            slaves_metrics[slave['id']]['tasks']['RUNNING'] = slave['TASK_RUNNING']
            slaves_metrics[slave['id']]['tasks']['STAGING'] = slave['TASK_STAGING']
            slaves_metrics[slave['id']]['tasks']['STARTING'] = slave['TASK_STARTING']
            slaves_metrics[slave['id']]['offered_resources']['cpus'] = slave['offered_resources']['cpus']
            slaves_metrics[slave['id']]['offered_resources']['mem'] = slave['offered_resources']['mem'] * 1024 * 1024
            slaves_metrics[slave['id']]['offered_resources']['disk'] = slave['offered_resources']['disk'] * 1024 * 1024
            slaves_metrics[slave['id']]['used_resources']['cpus'] = slave['used_resources']['cpus']
            slaves_metrics[slave['id']]['used_resources']['mem'] = slave['used_resources']['mem'] * 1024 * 1024
            slaves_metrics[slave['id']]['used_resources']['disk'] = slave['used_resources']['disk'] * 1024 * 1024
            slaves_metrics[slave['id']]['resources']['cpus'] = slave['resources']['cpus']
            slaves_metrics[slave['id']]['resources']['mem'] = slave['resources']['mem'] * 1024 * 1024
            slaves_metrics[slave['id']]['resources']['disk'] = slave['resources']['disk'] * 1024 * 1024
        return (dcos_metrics, slaves_metrics)

    def add_datapoints(self, data):
        if data:
            dcos_metrics, slaves_metrics = self.parse(data)
            for cluster in dcos_metrics.keys():
                for metrics_group in dcos_metrics[cluster].keys():
                    for metric_item in dcos_metrics[cluster][metrics_group].keys():
                        value = dcos_metrics[cluster][metrics_group][metric_item]
                        metric_name = "%s %s %s" % (cluster, metrics_group, metric_item)
                        if metrics_group == 'tasks':
                            unit = "tasks"
                        elif metric_item == 'cpus':
                            unit = "counts"
                        else:
                            unit = "bytes"
                        self.add_gauge_value(metric_name, unit, value)

            for slave in slaves_metrics.keys():
                for metrics_group in slaves_metrics[slave].keys():
                    for metric_item in slaves_metrics[slave][metrics_group].keys():
                        value = slaves_metrics[slave][metrics_group][metric_item]
                        metric_name = "%s %s %s" % (slave, metrics_group, metric_item)
                        if metrics_group == 'tasks':
                            unit = "tasks"
                        elif metric_item == 'cpus':
                            unit = "counts"
                        else:
                            unit = "bytes"
                        self.add_gauge_value(metric_name, unit, value)
        else:
            LOGGER.debug('Stats output: %r', data)
