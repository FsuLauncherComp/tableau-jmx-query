import argparse
import json
import logging
import os
import re
from datetime import datetime
from getpass import getpass
from typing import Dict, List

import requests
from jmxquery import JMXQuery, JMXConnection
from requests.packages.urllib3.exceptions import InsecureRequestWarning

OUTPUT_DIR = "output"


class JMX:
    @staticmethod
    def _clean_data_set(all_metrics: Dict):
        """
        Cleans the dataset to return a flattened json structure
        """

        def get_kpis(metric_set):
            cleaned_metrics = {
                "address": metric_set["address"],
                "node": metric_set["nodeId"],
                "port": metric_set["port"],
                "timestamp": metric_set["timestamp"],
            }
            for k, v in metric_set["metrics"].items():
                cleaned_metrics.update({"metricName": k, **v})
            return cleaned_metrics

        return [get_kpis(metric_set) for metric_set in all_metrics]

    def get_jmx_stats(self, nodes: List[Dict]):
        """
        Finds all JMX counters for the nodes given
        """
        ts = datetime.now().astimezone().replace(microsecond=0).isoformat()
        all_metrics = [
            self._get_jmx_stats(
                node.get("port"), node.get("address"), node.get("nodeId"), ts
            )
            for node in nodes
        ]
        return self._clean_data_set(all_metrics)

    @staticmethod
    def _get_jmx_stats(port: str, address: str, nodeId: str, ts: int):
        """
        Constructs the JMX queries and executes them against the ports found
        """
        results = {
            "nodeId": nodeId,
            "port": port,
            "address": address,
            "timestamp": ts,
            "metrics": {},
        }
        base_query = f"service:jmx:rmi:///jndi/rmi://{address}:{port}/jmxrmi"
        jmxConnection = JMXConnection(base_query)
        jmxQuery = [JMXQuery(mBeanName="*tableau*:*", metric_name="{name}")]
        metrics = jmxConnection.query(jmxQuery)

        for metric in metrics:
            jmx_metric = dict(
                zip(
                    ["jmxQuery", "serviceType", "serviceProcess", "serviceName"],
                    re.split(r"\,+\d+\d+\=", metric.mBeanName),
                ),
                **{
                    metric.attributeKey
                    if metric.attributeKey
                    else metric.attribute: metric.value
                },
            )
            if metric_name := results["metrics"].get(metric.metric_name):
                metric_name.update(jmx_metric)
            else:
                results["metrics"][metric.metric_name] = jmx_metric
        return results


class TSM:
    def __init__(self, server):
        self.server = self._format_server_address(server)
        self.session = None
        self._version = "0.5"

    @staticmethod
    def _format_server_address(server):
        """Return a formatted server string."""

        # Switch to https if http is specified explicitly
        if "http://" in server.lower():
            logging.info("TSM requires HTTPS. Updating the request URL.")
            server = server.replace("http://", "https://", 1)

        # Insert https if neither http nor https was specified
        if "https://" not in server.lower():
            server = f"https://{server}"

        # Use default port number if no port number was specified
        if ": " not in server:
            logging.info("No port number specified. Using 8850 as the default.")
            server = f"{server}:8850"

        return server

    def build_url(self, endpoint: str, params: List[str] = []) -> str:
        """Returns a formatted request URL."""
        query_string = "?" + "&".join(params) if params else ""
        return f"{self.server}/api/{self._version}/{endpoint}{query_string}"

    @staticmethod
    def process_response(resp):
        """Process the response for successful 2xx response codes, else raise an error."""
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 204:
            return {}
        else:
            raise SystemExit(
                "{0} error: {1}".format(
                    resp.status_code, resp.json()["error"]["detail"]
                )
            )

    def login(self, username: str, password: str) -> requests.Session:
        """Signs in to TSM."""

        url = self.build_url("login")
        headers = {"content-type": "application/json"}
        body = {"authentication": {"name": username, "password": password}}
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        session = requests.Session()
        resp = session.post(url, data=json.dumps(body), headers=headers, verify=False)
        self.process_response(resp)
        self.session = session

    def logout(self):
        """Signs out of TSM."""

        url = self.build_url("logout")
        resp = self.session.post(url)
        self.process_response(resp)

    def server_status(self):
        url = self.build_url("status")
        resp = self.session.get(url, verify=False)
        return self.process_response(resp)

    def get_node_info(self, node_id: str):
        url = self.build_url(f"nodes/{node_id}")
        resp = self.session.get(url, verify=False)
        return self.process_response(resp)

    def ports(self, job_timeout_seconds: int = None):
        params = (
            [f"jobTimeoutSeconds={job_timeout_seconds}"] if job_timeout_seconds else []
        )
        url = self.build_url("ports", params=params)
        resp = self.session.get(url, verify=False)
        return self.process_response(resp)

    def _get_jmx_instance(
        self, node_id: str, node_info: Dict, service: Dict, instance: Dict, port: Dict
    ) -> Dict:
        """Helper function to create a JMX instance dict."""
        return {
            "node_id": node_id,
            "address": node_info.get("address"),
            "node_addresses": node_info.get("addresses"),
            "service_name": service.get("serviceName"),
            "instance_id": instance.get("instanceId"),
            "port": port.get("port"),
        }

    def _process_instance_services(
        self, node_id: str, node_info: Dict, service: Dict, instance_services: List[str]
    ) -> List[Dict]:
        """Process instance services and return a list of JMX instances."""
        return (
            [
                self._get_jmx_instance(node_id, node_info, service, instance, port)
                for instance in service.get("instances", [])
                for port in instance.get("ports", [])
                if port.get("portType") == "jmx"
            ]
            if service.get("serviceName") in instance_services
            else []
        )

    def jmx_instances(
        self, instance_services: List[str] = ["vizqlserver", "backgrounder"]
    ) -> List[Dict]:
        """Finds JMX instances for the given instance services."""
        cluster_ports = self.ports().get("clusterPorts", {})
        nodes = cluster_ports.get("nodes", [])

        return [
            jmx_instance
            for node in nodes
            for node_info in [
                self.get_node_info(node_id=node.get("nodeId")).get("nodeInfo", {})
            ]
            for service in node.get("services", [])
            for jmx_instance in self._process_instance_services(
                node.get("nodeId"), node_info, service, instance_services
            )
        ]


def run(args):
    tsm_instance = TSM(args.server)
    tsm_instance.login(args.username, args.password)

    jmx_instances = tsm_instance.jmx_instances(
        instance_services=["vizqlserver", "backgrounder"]
    )

    jmx = JMX()

    jmx_stats = jmx.get_jmx_stats(jmx_instances)
    save_to_json("jmx_stats.json", jmx_stats)

    server_status = tsm_instance.server_status()
    save_to_json("server_status.json", server_status)


def save_to_json(filename, data):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, filename), "w") as outfile:
        json.dump(data, outfile)


def usage(args):
    parser = argparse.ArgumentParser(description="Tableau JMX Query Engine")
    parser.add_argument("--server", "-s", required=True, help="server address")
    parser.add_argument(
        "--username", "-u", required=True, help="username used to sign into TSM"
    )
    parser.add_argument(
        "--logging_level",
        "-l",
        choices=["debug", "info", "error"],
        default="error",
        help="desired logging level (set to error by default)",
    )
    return parser.parse_args(args)


def main():
    import sys

    args = usage(sys.argv[1:])
    args.password = getpass("Enter password: ")
    logging.basicConfig(level=args.logging_level.upper())
    run(args)


if __name__ == "__main__":
    main()
