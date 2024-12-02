import asyncio
import logging
from pymodbus.client.sync import ModbusTcpClient  # Chỉnh sửa lại import tại đây
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.constants import Endian

# Cài đặt logger
logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Cấu hình IP và thông số
smart_logger = ("127.0.0.1", "127.0.0.2")
meter_addr = 32278
# power_address = 40525
# control_address = 40420

power_address = 32280  # test
control_address = 32282

num_of_inv = [5, 7]
max_power = 110  # kwh

normal = 0.01 * max_power
upper_limit = 1.5 * normal
lower_limit = 0.5 * normal

power_grid = []
power_inv = []


# Hàm giải mã dữ liệu
def value_decode(registers, typeString, size, byte_order, word_order, gain):
    decoder = BinaryPayloadDecoder.fromRegisters(
        registers, byteorder=byte_order, wordorder=word_order
    )
    value = None
    if typeString == "int16":
        value = decoder.decode_16bit_int()
    elif typeString == "uint16":
        value = decoder.decode_16bit_uint()
    elif typeString == "int32":
        value = decoder.decode_32bit_int()
    elif typeString == "uint32":
        value = decoder.decode_32bit_uint()
    elif typeString == "float16":
        value = decoder.decode_16bit_float()
    elif typeString == "float32":
        value = decoder.decode_32bit_float()
    elif typeString == "string":
        value = decoder.decode_string(size).decode()
    else:
        value = "Invalid type"
    return value / 10**gain


# Hàm kết nối Modbus
async def connect_modbus_client(ip, port=502):
    client = ModbusTcpClient(ip, port)
    if not client.connect():
        logger.error(f"Unable to connect to Modbus client at {ip}")
        return None
    return client


# Hàm đọc dữ liệu từ đồng hồ điện
async def read_meter_data():
    global power_grid
    tasks = []
    power_grid.clear()
    power_inv.clear()

    for ip in smart_logger:

        client = await connect_modbus_client(ip)

        meter = client.read_holding_registers(meter_addr, 2, unit=1)
        power_grid.append(
            value_decode(meter.registers, "int32", 2, Endian.Big, Endian.Big, 3)
        )

        inverter = client.read_holding_registers(power_address, 2, unit=1)
        power_inv.append(
            value_decode(inverter.registers, "int32", 2, Endian.Big, Endian.Big, 3)
        )

        client.close()

    print(power_grid)
    logger.info(f"The total value of the main meter is = {sum(power_grid)}")
    logger.info(f"The total power output of the SmartLogger is = {power_inv}")


async def zero_export_logic():

    switch = ""
    global power_grid

    # for ip, i in zip(smart_logger, range(len(smart_logger)+1)):
    for i in range(len(smart_logger)):

        if int(power_grid[i]) < int(lower_limit * num_of_inv[i]):
            switch += "a"

        elif int(power_grid[i]) > int(upper_limit * num_of_inv[i]):
            switch += "b"

        else:
            switch += "c"

    print("switch = ''", switch)

    match switch:
        case "aa":  # cả 2 đẩy lưới => giảm tải theo tỉ lệ

            total_red = normal * sum(num_of_inv) - sum(power_grid)
            for ip, i in zip(smart_logger, range(len(smart_logger) + 1)):

                # power_red = (normal*num_of_inv[i] - power_grid[i])
                power_red = total_red * num_of_inv[i] / sum(num_of_inv)

                # buffer tải giảm nhanh
                if power_red <= 0.02 * max_power * num_of_inv[i]:
                    power_red = power_red * 1.25
                elif power_red <= 0.05 * max_power * num_of_inv[i]:
                    power_red = power_red * 1.5
                elif power_red <= 0.1 * max_power * num_of_inv[i]:
                    power_red = power_red * 1.75
                elif power_red <= 0.2 * max_power * num_of_inv[i]:
                    power_red = power_red * 2
                await write_inverter_data(ip, inc=None, red=power_red, set0=False)

        case "bb" | "bc" | "cb":  # cả 2 thiếu => tăng ko quá 20%

            total_inc = sum(power_grid) - normal * sum(num_of_inv)
            print(total_inc, sum(power_grid))
            for ip, i in zip(smart_logger, range(len(smart_logger) + 1)):

                # power_inc = (power_grid[i] - normal*num_of_inv[i])

                power_inc = total_inc * num_of_inv[i] / sum(num_of_inv)
                power_inc = min(power_inc, 0.2 * num_of_inv[i] * max_power)
                await write_inverter_data(ip, inc=power_inc, red=None, set0=False)
                print(power_inc)

        case "ab" | "ac":  # 1 dư, 2 thiếu

            client = await connect_modbus_client(smart_logger[1])
            if client is None:
                return
            data = client.read_holding_registers(control_address, 2, unit=1)
            old_set = value_decode(
                data.registers, "uint32", 2, Endian.Big, Endian.Big, 3
            )

            power_mov = power_grid[1] - normal * num_of_inv[1]  # lượng thiếu
            power_exp = normal * num_of_inv[0] - power_grid[0]  # lượng dư

            # bù khi không đạt 80% giá trị set hoặc thâm hụt 5% tổng công suất max
            if (
                power_inv[1] <= 0.8 * old_set
                or old_set - power_inv[1] >= num_of_inv[1] * max_power * 0.05
            ) and switch == "ab":

                # bù cho phần thiếu
                if power_exp >= power_mov:  # dư nhiều hơn thiếu

                    power_red = power_exp - power_mov
                    await write_inverter_data(
                        smart_logger[0], inc=None, red=power_red, set0=False
                    )  # giới hạn tải vừa cái thiếu
                    await write_inverter_data(
                        smart_logger[1], inc=None, red=None, set0=True
                    )  # hạn lại giá trị tải thực

                elif power_exp <= power_mov:  # dư ít hơn

                    power_sub = power_mov - power_exp
                    power_inc = min(power_exp, 0.2 * num_of_inv[0] * max_power)
                    await write_inverter_data(
                        smart_logger[0], inc=power_inc, red=None, set0=False
                    )
                    await write_inverter_data(
                        smart_logger[1], inc=power_sub, red=None, set0=False
                    )
            else:
                # chỉ hạn phần dư, ko bù phần thiếu
                power_red = normal * num_of_inv[0] - power_grid[0]
                power_red = min(power_red, 0.2 * num_of_inv[0] * max_power)
                await write_inverter_data(
                    smart_logger[0], inc=None, red=power_red, set0=False
                )

        case "ba" | "ca":  # 1 thiếu, 2 dư

            client = await connect_modbus_client(smart_logger[0])
            if client is None:
                return
            data = client.read_holding_registers(control_address, 2, unit=1)
            old_set = value_decode(
                data.registers, "uint32", 2, Endian.Big, Endian.Big, 3
            )

            power_mov = power_grid[0] - normal * num_of_inv[0]  # lượng thiếu
            power_exp = normal * num_of_inv[1] - power_grid[1]  # lượng dư

            # bù khi không đạt 80% giá trị set hoặc thâm hụt 5% tổng công suất max
            if (
                power_inv[0] <= 0.8 * old_set
                or old_set - power_inv[0] >= num_of_inv[0] * max_power * 0.05
            ) and switch == "ba":

                # bù cho phần thiếu
                if power_exp >= power_mov:  # dư nhiều hơn thiếu

                    power_red = power_exp - power_mov
                    await write_inverter_data(
                        smart_logger[1], inc=None, red=power_red, set0=False
                    )  # giới hạn tải vừa cái thiếu
                    await write_inverter_data(
                        smart_logger[0], inc=None, red=None, set0=True
                    )  # hạn lại giá trị tải thực

                elif power_exp <= power_mov:  # dư ít hơn

                    power_sub = power_mov - power_exp
                    power_inc = min(power_exp, 0.2 * num_of_inv[1] * max_power)
                    await write_inverter_data(
                        smart_logger[1], inc=power_inc, red=None, set0=False
                    )
                    await write_inverter_data(
                        smart_logger[0], inc=power_sub, red=None, set0=False
                    )
            else:
                # chỉ hạn phần dư, ko bù phần thiếu
                power_red = normal * num_of_inv[1] - power_grid[1]
                power_red = min(power_red, 0.2 * num_of_inv[1] * max_power)
                await write_inverter_data(
                    smart_logger[1], inc=None, red=power_red, set0=False
                )

        case "cc":  # chạy ổn định
            logger.info(f"The system operates stably")
            return


# Hàm ghi dữ liệu vào bộ biến tần
async def write_inverter_data(ip, inc, red, set0):

    builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)

    # try:
    client = await connect_modbus_client(ip)
    if client is None:
        return
    data = client.read_holding_registers(control_address, 2, unit=1)
    old_set = value_decode(data.registers, "uint32", 2, Endian.Big, Endian.Big, 3)
    if data.isError():
        raise ValueError(f"Error reading control register at {ip}")

    stt = smart_logger.index(ip)

    if inc:
        print(inc)

        new_data = int(
            round(min(power_inv[stt] + inc, max_power * num_of_inv[stt]), 3) * 1000
        )

        if old_set >= max_power * num_of_inv[stt]:

            logger.warning(f"The inverter {ip} has reached its maximum capacity.")
            return

        builder.add_32bit_uint(new_data)
        client.write_registers(control_address, builder.to_registers(), unit=1)
        logger.info(f"Increased power of inverter {ip} to {new_data/1000 }kWh")

    elif red:
        print(red)
        new_data = int(round((max(power_inv[stt] - red, 0)), 3) * 1000)

        if old_set <= 0 and power_inv[stt] <= 0.02 * max_power * num_of_inv[stt]:

            logger.warning("The inverter has reached its minimum capacity.")
            return

        builder.add_32bit_uint(new_data)
        client.write_registers(control_address, builder.to_registers(), unit=1)
        logger.info(f"Decreased power of inverter {ip} to {new_data/1000 }kWh")

    elif set0:

        builder.add_32bit_uint(int(power_inv[stt] * 1000))  # kWh
        client.write_registers(control_address, builder.to_registers(), unit=1)
        logger.info(f"Decreased power of inverter {ip} to {power_inv[stt] }kWh")

    client.close()


# except Exception as e:
#     logger.error(f"Unable to write power value on the {ip} inverter: {e}")


# Hàm vòng lặp chính
async def main_loop():
    # while True:3
    # Đọc dữ liệu đồng hồ và inverter đồng thời
    await asyncio.gather(
        read_meter_data(),
        # write_inverter_data(),
        zero_export_logic(),
    )
    await asyncio.sleep(2)


if __name__ == "__main__":
    logger.info("Starting Zero Export Logic Controller")
    asyncio.run(main_loop())
