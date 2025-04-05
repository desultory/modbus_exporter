from prometheus_exporter import Exporter, Metric
from pymodbus.client.serial import AsyncModbusSerialClient
from pymodbus.exceptions import ConnectionException


class ModbusExporter(Exporter):
    """Modbus exporter class for prometheus metrics."""

    def __init__(self, *args, **kwargs):
        self.endpoints = []
        kwargs["listen_port"] = kwargs.pop("listen_port", 9502)
        super().__init__(*args, **kwargs)

    async def startup_tasks(self, *args, **kwargs):
        self.client = AsyncModbusSerialClient(
            method="rtu",
            port=self.serial_port,
            baudrate=self.serial_baudrate,
            timeout=self.serial_timeout,
            parity=self.serial_parity,
            bytesize=self.serial_bytesize,
            stopbits=self.serial_stopbits,
        )

    def read_config(self):
        """Ensure modbus config is defined, use that to define endpoints, which will then read the config."""
        super().read_config()
        if "modbus" not in self.config:
            raise ValueError("No Modbus config defined.")

        self.serial_port = self.config["modbus"].get("serial_port", "/dev/ttyUSB0")
        self.serial_baudrate = self.config["modbus"].get("serial_baudrate", 9600)
        self.serial_timeout = self.config["modbus"].get("serial_timeout", 1)
        self.serial_parity = self.config["modbus"].get("serial_parity", "N")
        self.serial_bytesize = self.config["modbus"].get("serial_bytesize", 8)
        self.serial_stopbits = self.config["modbus"].get("serial_stopbits", 1)
        self.modbus_slave = self.config["modbus"].get("slave_address", 1)
        self.modbus_registers = self.config["modbus"].get("registers", {})

    async def get_modbus_values(self):
        """ Iterate over all modbus register sections.
        Key name is the help text, value is the register address.
        The name of the sectoin is used for the metric name.

        The address and slave address are added as labels.
        """
        metrics = []
        for metric_list, metric_info in self.modbus_registers.items():
            for name, address in metric_info.items():
                try:
                    value = await self.client.read_holding_registers(address, 1, slave=self.modbus_slave)
                except ConnectionException as e:
                    self.logger.critical("Connection error: %s", e)
                    continue

                if value.isError():
                    self.logger.error("Error reading register %s: %s", address, value)
                    continue
                self.logger.info(f"[{self.modbus_slave}] {name}: {value.registers[0]}")
                metric = Metric(
                    name=metric_list,
                    labels={"slave": str(self.modbus_slave), "address": str(address)},
                    value=value.registers[0],
                    type="gauge",
                    help=name,
                    logger=self.logger,
                )
                metrics.append(metric)

        return metrics

    async def get_metrics(self, label_filter={}):
        """Get metrics list from each endpoint, add them together"""
        metric_list = await super().get_metrics(label_filter=label_filter)

        if not self.client.connected:
            try:
                await self.client.connect()
            except ConnectionException as e:
                self.logger.critical("Connection error: %s", e)
                return metric_list

        metric_list += await self.get_modbus_values()

        self.logger.debug("Got %d metrics", len(metric_list))
        self.metrics = metric_list
        return metric_list
