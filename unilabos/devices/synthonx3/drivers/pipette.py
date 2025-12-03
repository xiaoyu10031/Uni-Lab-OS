import time
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List
from unilabos.devices.synthonx3.drivers.bus_rs485 import SharedRS485Bus

logger = logging.getLogger("liquid_station.pipette_driver")

class SOPAConfig:
    address: int = 4          # 固定为 4
    timeout: float = 2.0

class SOPAPipetteYYQ:
    def __init__(self, bus: SharedRS485Bus, config: SOPAConfig = SOPAConfig()):
        self.bus = bus
        self.config = config
        self.is_initialized = False

    def _send_and_read(self, cmd: str, delay_before_read: float = 0.2) -> str:
        '''
        发送一条命令并在同一把锁内读取响应，确保事务原子。

        参数:
            cmd: 不含地址/结束符的命令正文（如 "HE"、"RE"、"P100"）
            delay_before_read: 读响应前的等待时间，给设备处理/返回留出缓冲

        返回:
            解码后的响应字符串（若无数据则为空字符串）
        '''
        address = str(self.config.address)
        full_cmd = f"/{address}{cmd}E".encode("ascii")
        checksum = bytes([sum(full_cmd) & 0xFF])
        payload = full_cmd + checksum
        with self.bus.lock:
            self.bus.reset_input()                # 发前清输入缓冲，丢弃旧数据
            self.bus.write(payload)
            logger.debug(f"[YYQ] TX: {payload!r}")
            time.sleep(delay_before_read)         # 等待设备响应
            data = b""
            if self.bus.serial.in_waiting:
                data = self.bus.serial.read_all()
        txt = data.decode(errors="ignore")
        if txt:
            logger.debug(f"[YYQ] RX: {txt!r}")
        return txt

    def initialize(self) -> bool:
        try:
            logger.info("初始化移液枪中...")
            self._send_and_read("HE")
            time.sleep(10)
            self.is_initialized = True
            logger.info("初始化完成")
            return True
        except Exception as e:
            logger.error(f"初始化失败: {e}")
            return False

    def eject_tip(self) -> bool:
       try:
           self._send_and_read("RE")
           time.sleep(1)
           logger.info("枪头已弹出")
           return True
       except Exception as e:
           logger.error(f"弹出枪头失败: {e}")
           return False

    def aspirate(self, volume_uL: float):
        try:
            vol = int(volume_uL)
            logger.info(f"吸液 {vol} µL...")
            self._send_and_read(f"P{vol}")
            time.sleep(max(0.2, vol / 200.0))
            logger.info("吸液完成")
        except Exception as e:
            logger.error(f"吸液失败: {e}")

    def dispense(self, volume_uL: float):
        try:
            vol = int(volume_uL)
            logger.info(f"排液 {vol} µL...")
            self._send_and_read(f"D{vol}")
            time.sleep(max(0.2, vol / 200.0))
            logger.info("排液完成")
        except Exception as e:
            logger.error(f"排液失败: {e}")
    
    def set_max_speed(self, speed: int):
        """设置最高速度 (s 指令)"""
        try:
            self._send_and_read(f"s{speed}")
            time.sleep(1)
            logger.info("设置完成")
        except Exception as e:
            logger.error(f"设置失败: {e}")

    def set_start_speed(self, speed: int):
        """设置启动速度 (b 指令)"""
        try:
            self._send_and_read(f"b{speed}")
            time.sleep(1)
            logger.info("设置完成")
        except Exception as e:
            logger.error(f"设置失败: {e}")

    def set_cutoff_speed(self, speed: int):
        """设置断流/停止速度 (c 指令)"""
        try:
            self._send_and_read(f"c{speed}")
            time.sleep(1)
            logger.info("设置完成")
        except Exception as e:
            logger.error(f"设置失败: {e}")

    def set_acceleration(self, accel: int):
        """设置加速度 (a 指令)"""
        try:
            self._send_and_read(f"a{accel}")
            time.sleep(1)
            logger.info("设置完成")
        except Exception as e:
            logger.error(f"设置失败: {e}")
    
    def get_status(self) -> str:
        """查询状态 (Q 指令)"""
        return self._send_and_read("Q", delay_before_read=0.1)

    def get_tip_status(self) -> bool:
        """
        查询枪头状态 (Q28)
        返回: True(有枪头), False(无枪头)
        """
        resp = self._send_and_read("Q28", delay_before_read=0.1)
        if resp:
            # 响应通常包含 "T1"(有) 或 "T0"(无)
            if "T1" in resp: return True
            if "T0" in resp: return False
        return False # 默认无枪头或通信失败
    