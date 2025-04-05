#!/usr/bin/env python3

from prometheus_exporter import DEFAULT_EXPORTER_ARGS
from zenlib.util import get_kwargs

from modbus_exporter import ModbusExporter


def main():
    kwargs = get_kwargs(
        package=__package__, description="Modbus Exporter for Prometheus", arguments=DEFAULT_EXPORTER_ARGS
    )

    exporter = ModbusExporter(**kwargs)
    exporter.start()


if __name__ == "__main__":
    main()
