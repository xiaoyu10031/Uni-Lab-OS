import time
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List
from unilabos.devices.synthonx3.drivers.bus_rs485 import SharedRS485Bus

logger = logging.getLogger("liquid_station.xyz_driver")

class MotorStatus(Enum):
    STANDBY = 0x0000   # 待机/到位
    RUNNING = 0x0001  # 运行中
    COLLISION_STOP = 0x0002   # 碰撞停
    FORWARD_LIMIT_STOP = 0x0003  # 正光电停
    REVERSE_LIMIT_STOP = 0x0004  # 反光电停


class ModbusException(Exception):
    pass


@dataclass
class MotorPosition:
    steps: int
    speed: int
    current: int
    status: MotorStatus


class XYZModbus:
    """
    Minimal Modbus RTU helper bound to the shared bus.

    简化容错：ignore_crc_error=True 时，CRC 重试耗尽后直接忽略继续，不再统计次数。
    """

    REG_STATUS = 0x00 
    '''
    读取电机状态：
    0000H-待机中或到达位置
    0001H-正在运行
    0002H-碰撞停
    0003H-正光电停
    0004H-反光电停
    '''

    REG_POSITION_HIGH = 0x01 # 实际步数高位
    REG_POSITION_LOW = 0x02 # 实际步数低位
    REG_ACTUAL_SPEED = 0x03 # 实际速度
    REG_EMERGENCY_STOP = 0x04 # 急停指令
    REG_CURRENT = 0x05 # 电流(mA)
    REG_ENABLE = 0x06 # 使能

    # 位置模式
    REG_TARGET_POSITION_HIGH = 0x10 # 目标步数高位
    REG_TARGET_POSITION_LOW = 0x11 # 目标步数低位
    REG_POSITION_SPEED = 0x13 # 速度(rpm)
    REG_POSITION_ACCELERATION = 0x14 # 加速度(rpm/s)
    REG_POSITION_PRECISION = 0x15 # 精度(步数)

    # 速度模式
    REG_SPEED_MODE_SPEED = 0x61 # 速度(rpm)
    REG_SPEED_MODE_ACCELERATION = 0x62 # 加速度(rpm/s)

    def __init__(self, bus: SharedRS485Bus, ignore_crc_error: bool = False):
        self.bus = bus
        self.ignore_crc_error = ignore_crc_error

    def set_ignore_crc(self, flag: bool):
        self.ignore_crc_error = bool(flag)

    @staticmethod
    def _crc16(data: bytes) -> bytes:
        '''
        计算 Modbus CRC16 校验，返回小端 2 字节。

        参数:
            data: 待校验的数据字节序列（不含 CRC 部分）

        返回:
            2 字节的小端序 CRC 校验值
        '''
        crc = 0xFFFF   # 初始值
        for b in data:
            crc ^= b    # 先与当前字节异或
            for _ in range(8):    # 逐位处理 8 次
                if crc & 0x0001:   # 若最低位为 1，右移并异或多项式
                    crc >>= 1
                    crc ^= 0xA001
                else:    # 否则仅右移
                    crc >>= 1
        return crc.to_bytes(2, "little")    # 以小端序输出 2 字节

    def _xfer(self, slave: int, payload: bytes, retries: int = 3,delay_before_read: float = 0.2) -> bytes:
        '''
        发送 Modbus RTU 请求并接收响应，内含重试。
        协议：帧 = 从站地址(1) + 功能码/数据(payload) + CRC16(小端)
        参数:
            slave: 从站地址(1 字节整数)
            payload: 功能码及数据段(不含从站地址与 CRC)
            retries: 失败重试次数
            delay_before_read: 读响应前的等待时间，给设备处理/返回留出缓冲
        返回:
            完整响应帧(含地址/功能码/数据/CRC),校验通过；异常则抛出 ModbusException
        '''
        def hx(data: bytes) -> str:
            return " ".join(f"{b:02X}" for b in data)
    
        req = bytes([slave]) + payload  # 请求头：从站地址 + 功能码/数据
        frame = req + self._crc16(req)  # 加上 CRC16 组成完整帧
        fn_req = payload[0]   # 记录功能码

        logger.info("TX slave=%d fn=0x%02X frame=%s", slave, fn_req, hx(frame))

        for attempt in range(1, retries + 1):
            try:
                with self.bus.lock:   # 发前持锁，防止并发写/清缓冲
                    if not self.bus.serial or not self.bus.serial.is_open:
                        raise ModbusException("Bus not open")

                    self.bus.reset_input()    # 清输入缓冲，丢弃旧数据
                    self.bus.write(frame)     # 发送请求帧

                    time.sleep(delay_before_read) # 等待相应

                    base = 0.30 + 0.15*(attempt-1)   # 随重试递增的基础超时
                    header = self.bus.read_exact(2, overall_timeout=base)
                    if len(header) < 2:
                        raise ModbusException("No response")

                    addr, fn = header[0], header[1]  # 解析地址和功能码
                    if addr != slave:
                        # 把这一帧当成串扰/回波，丢弃后继续本次尝试
                        time.sleep(0.005)
                        continue

                    if (fn & 0x80) != 0:    # 异常响应帧
                        rest = self.bus.read_exact(3, overall_timeout=base)
                        resp = header + rest
                        # 记录响应（原始十六进制，含 CRC）
                        logger.info(
                            "RX attempt %d/%d slave=%d fn=0x%02X resp=%s",
                            attempt, retries, slave, fn_req, hx(resp)
                        )
                        if len(rest) < 3:
                            raise ModbusException("Short exception response")
                        if resp[-2:] != self._crc16(resp[:-2]):
                            logger.warning(f"CRC mismatch (exception response) attempt {attempt}/{retries} slave={slave} fn=0x{fn_req:02X}")
                            if attempt >= retries:
                                if self.ignore_crc_error:
                                    logger.error("CRC mismatch 重试耗尽已忽略 (风险：数据未校验)")
                                    return resp  # 返回未校验异常帧
                                raise ModbusException("CRC mismatch (exception)")
                            time.sleep(0.005)
                            continue
                        ex_code = resp[2]
                        raise ModbusException(f"Modbus exception: 0x{ex_code:02X}")

                    if fn == 0x03:    # 读保持寄存器
                        bc_b = self.bus.read_exact(1, overall_timeout=base) # 读取字节数
                        if len(bc_b) < 1:
                            raise ModbusException("Short response (no byte count)")
                        bc = bc_b[0]   # 数据区字节数
                        data_crc = self.bus.read_exact(bc + 2, overall_timeout=base + 0.20) # 根据字节数读取剩余所有数据
                        resp = header + bc_b + data_crc # 组成完整响应
                        # 记录响应（原始十六进制，含 CRC）
                        logger.info(
                            "RX attempt %d/%d slave=%d fn=0x%02X resp=%s",
                            attempt, retries, slave, fn_req, hx(resp)
                        )
                        if len(data_crc) < bc + 2:
                            raise ModbusException("Short response (payload)")
                        
                    elif fn in (0x06, 0x10):   # 单寄存器写/多寄存器写
                        rest = self.bus.read_exact(6, overall_timeout=base + 0.20)
                        resp = header + rest
                        # 记录响应（原始十六进制，含 CRC）
                        logger.info(
                            "RX attempt %d/%d slave=%d fn=0x%02X resp=%s",
                            attempt, retries, slave, fn_req, hx(resp)
                        )
                        if len(rest) < 6:
                            raise ModbusException("Short response")
                    else:   # 其他功能码兜底
                        tail = self.bus.read_exact(254, overall_timeout=base + 0.30)
                        resp = header + tail
                        # 记录响应（原始十六进制，含 CRC）
                        logger.info(
                            "RX attempt %d/%d slave=%d fn=0x%02X resp=%s",
                            attempt, retries, slave, fn_req, hx(resp)
                        )
                        if len(resp) < 3:
                            raise ModbusException("Short response")
                        
                    # CRC 校验
                    if resp[-2:] != self._crc16(resp[:-2]):
                        logger.warning(f"CRC mismatch (attempt {attempt}/{retries}) slave={slave} fn=0x{fn_req:02X}")
                        if attempt >= retries:
                            if self.ignore_crc_error:
                                logger.error("CRC mismatch 重试耗尽已忽略 (风险：数据未校验)")
                                return resp  # 直接返回未校验帧
                            raise ModbusException("CRC mismatch")
                        time.sleep(0.005)
                        continue

                    if resp[1] != fn_req:
                        raise ModbusException(f"Unexpected function: {resp[1]:02X} (!={fn_req:02X})")

                    return resp  # 成功
            except ModbusException as e:
                logger.warning(f"Attempt {attempt} failed with error: {e}")
                if attempt == retries:
                    logger.warning("Max retries reached.")
                else:
                    logger.warning(f"Retrying... {attempt}/{retries}")
                
    def read_regs(self, slave: int, addr: int, count: int) -> List[int]: 
        '''
        读取寄存器
        格式：# 从站地址+功能码(03)+寄存器地址+寄存器个数+校验码

        参数:
            slave: 从站地址,1 bytes
            addr: 寄存器地址,2 bytes
            conut: 寄存器个数,2 bytes

        返回:
            vals[int]:数据的整数列表,按字节划分
        '''
        fn = 0x03 # 功能码
        payload = bytes([fn]) + addr.to_bytes(2, "big") + count.to_bytes(2, "big") # 功能码+寄存器地址+寄存器个数
        resp = self._xfer(slave, payload) # 发送数据帧
        if not resp:
            # 如果通信失败（如超时或校验错误），_xfer 返回 None
            # 此时直接抛出异常，避免后续 resp[2] 报错
            raise ModbusException(f"read_regs failed: No response from slave {slave} (addr={addr})")
        byte_count = resp[2] #读取响应段中的字节数
        vals = []
        for i in range(0, byte_count, 2): #提取响应中的数据段
            vals.append(int.from_bytes(resp[3 + i:5 + i], "big"))
        return vals

    def write_reg(self, slave: int, addr: int, val: int) -> bool:
        '''
        写入单个寄存器
        格式：# 从站地址+功能码(06)+寄存器地址+数据+校验码

        参数:
            slave: 从站地址,1 bytes
            addr: 寄存器地址,2 bytes
            val: 数据,2 bytes

        返回:
            报错: False
            正常: True
        '''
        fn = 0x06 #功能码
        payload = bytes([fn]) + addr.to_bytes(2, "big") + val.to_bytes(2, "big")  # 功能码+寄存器地址+数据
        try:
            resp = self._xfer(slave, payload) # 发送数据帧
        except ModbusException as e:
            logger.warning(f"write_reg: ModbusException slave={slave} addr={addr} val={val}: {e}")
            return False
        except Exception as e:
            logger.error(f"write_reg: unexpected error: {e}")
            return False

        if not resp:
            logger.warning(f"write_reg: no response slave={slave} addr={addr}")
            return False
        return len(resp) >= 8 and resp[1] == fn

    def write_regs(self, slave: int, start: int, values: List[int]) -> bool: 
        '''
        写入多个寄存器
        格式：# 从站地址+功能码(10)+寄存器地址+寄存器个数+字节数+数据+校验码

        参数:
            slave: 从站地址,1 bytes
            start: 寄存器地址,2 bytes
            values: 数据,n bytes

        返回:
            报错: False
            正常: True
        '''
        fn = 0x10 #功能码
        bc = len(values) * 2 #计算字节数
        payload = bytes([fn]) + start.to_bytes(2, "big") + len(values).to_bytes(2, "big") + bytes([bc]) #功能码(10)+寄存器地址+寄存器个数+字节数
        for v in values:
            payload += v.to_bytes(2, "big") # 加上数据
        try:
            resp = self._xfer(slave, payload)  # 发送数据帧
        except ModbusException as e:
            logger.warning(f"write_regs: ModbusException slave={slave} start={start} vals={values}: {e}")
            return False
        except Exception as e:
            logger.error(f"write_regs: unexpected error: {e}")
            return False

        if not resp:
            logger.warning(f"write_regs: no response slave={slave} start={start}")
            return False
        return len(resp) >= 8 and resp[1] == fn
