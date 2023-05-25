# Installation

1. Create a virual environment
2. Pip install from requirements.txt
3. Ensure your JAVA_HOME environment variable is set

### Running

CLI tool requires you to pass in arguments for your server, as well as, your username and password used for TSM.

Example: python main.py -s https://corp.tableau.com -u username -p password

### How it works

Queries the TSM REST API to obtain the JMX Ports for different nodes/services.

It takes that data and constructs JMX queries using the JMXQuery Python library to gather various metrics, such as concurrent sessions.

This data is returned in JSON.

## Disclaimer

This is not good code. It probably shouldn't be used in production. But it works.