from prometheus_exporter import Exporter, Metric
from zenlib.util.colorize import colorize as c_
from zenlib.util import pretty_print
from pymodbus.client.serial import AsyncModbusSerialClient
from pymodbus.client.tcp import AsyncModbusTcpClient
from pymodbus.exceptions import ConnectionException
from pymodbus.payload import BinaryPayloadDecoder


DATA_TYPES = ["int16", "uint16", "int32", "uint32", "float32"]

DECODER_MAP = {
    "int16": BinaryPayloadDecoder.decode_16bit_int,
    "uint16": BinaryPayloadDecoder.decode_16bit_uint,
    "int32": BinaryPayloadDecoder.decode_32bit_int,
    "uint32": BinaryPayloadDecoder.decode_32bit_uint,
    "float32": BinaryPayloadDecoder.decode_32bit_float,
}


class ModbusExporter(Exporter):
    """Modbus exporter class for prometheus metrics."""

    def __init__(self, *args, **kwargs):
        self.endpoints = []
        kwargs["listen_port"] = kwargs.pop("listen_port", 9502)
        super().__init__(*args, **kwargs)

    async def startup_tasks(self, *args, **kwargs):
        if self.mode == "tcp":
            self.client = AsyncModbusTcpClient(
                host=self.transport_config["host"],
                port=self.transport_config["port"],
                timeout=self.timeout,
            )

        elif self.mode == "rtu":
            self.client = AsyncModbusSerialClient(
                port=self.transport_config["port"],
                baudrate=self.transport_config["baudrate"],
                timeout=self.timeout,
                parity=self.transport_config["parity"],
                bytesize=self.transport_config["bytesize"],
                stopbits=self.transport_config["stopbits"],
            )
        else:
            raise ValueError("Invalid Modbus mode defined, must be 'tcp' or 'rtu'.")

    def read_config(self):
        """Ensure modbus config is defined, use that to define endpoints, which will then read the config."""
        super().read_config()
        if "modbus" not in self.config:
            raise ValueError("No Modbus config defined.")

        if "mode" not in self.config["modbus"]:
            self.logger.warning("No Modbus mode defined, defaulting to TCP")
            self.mode = "tcp"
        else:
            self.mode = self.config["modbus"]["mode"].lower()

        if self.mode not in ["tcp", "rtu"]:
            raise ValueError("Invalid Modbus mode defined, must be 'tcp' or 'rtu'.")

        self.transport_config = self.config["modbus"][self.mode]
        if self.mode == "rtu":
            self.transport_config["port"] = self.transport_config.get("port", "/dev/ttyUSB0")
            self.transport_config["baudrate"] = self.transport_config.get("baudrate", 9600)
            self.transport_config["timeout"] = self.transport_config.get("timeout", 1)
            self.transport_config["parity"] = self.transport_config.get("parity", "N")
            self.transport_config["bytesize"] = self.transport_config.get("bytesize", 8)
            self.transport_config["stopbits"] = self.transport_config.get("stopbits", 1)
        elif self.mode == "tcp":
            self.transport_config["host"] = self.transport_config.get("host", "127.0.0.1")
            self.transport_config["port"] = self.transport_config.get("port", 502)

        self.logger.info(f"[{c_(self.mode.upper(), 'blue')}] Transport config: {pretty_print(self.transport_config)}")

        self.timeout = self.config["modbus"].get("timeout", 1)
        self.device_id = self.config["modbus"].get("device_id", 1)
        self.modbus_registers = self.config["modbus"].get("registers", {})


        for register_list in self.modbus_registers.values():
            for name, address in register_list.items():
                if ":" in name:
                    name, data_type = name.split(":")
                    data_type = data_type.strip().lower()
                else:
                    data_type = "int16"

                if data_type not in DATA_TYPES:
                    raise NotImplementedError(f"Data type {data_type} for register {name} is not implemented, must be one of {', '.join(DATA_TYPES)}.")

    async def get_modbus_values(self):
        """ Iterate over all modbus register sections.
        Key name is the help text, value is the register address.
        The name of the sectoin is used for the metric name.

        The register address and device ID are added as labels.
        """
        metrics = []
        for metric_list, metric_info in self.modbus_registers.items():
            for name, address in metric_info.items():
                if ":" in name:
                    name, data_type = name.split(":")
                    data_type = data_type.strip().lower()
                else:
                    data_type = "int16"

                if "16" in data_type:
                    count = 1
                elif "32" in data_type:
                    count = 2

                try:
                    value = await self.client.read_holding_registers(address=address, count=count, device_id=self.device_id)
                except ConnectionException as e:
                    self.logger.critical("Connection error: %s", e)
                    continue

                if value.isError():
                    self.logger.error("Error reading register %s: %s", address, value)
                    continue

                decoded_value = DECODER_MAP[data_type](value.registers)

                self.logger.info(f"[{self.device_id}] {name}: {value.registers[0]}")
                metric = Metric(
                    name=metric_list,
                    labels={"device_id": str(self.device_id), "address": str(address)},
                    value=decoded_value,
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
