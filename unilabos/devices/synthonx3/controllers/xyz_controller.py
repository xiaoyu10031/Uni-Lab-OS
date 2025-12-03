import time
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List

from unilabos.devices.synthonx3.drivers.bus_rs485 import SharedRS485Bus
from unilabos.devices.synthonx3.drivers.motor_modbus import XYZModbus, MotorPosition, MotorStatus, ModbusException

# logger = logging.getLogger("liquid_station.xyz_driver")

class MotorAxis(Enum):
    X = 1
    Y = 2
    Z = 3

@dataclass
class MachineConfig:

    # 步数和mm之间的转化
    steps_per_mm_x: float = 204.8
    steps_per_mm_y: float = 204.8
    steps_per_mm_z: float = 3276.8

    # 最大行程(mm)
    max_travel_x: float = 340.0
    max_travel_y: float = 250.0
    max_travel_z: float = 250.0

    #安全Z轴高度
    safe_z_height: float = 5.0
    z_approach_height: float = 5.0

    #全局回零设置
    homing_speed: int = 100
    homing_accel: int = 150
    homing_timeout: float = 30.0
    safe_clearance: float = 1.0 # 安全退让距离(mm)
    position_stable_time: float = 3.0
    position_check_interval: float = 0.2

    #运行速度
    default_speed: int = 300
    default_acceleration: int = 500

    #各轴回零设置
    homing_speed_x: int = 50
    homing_speed_y: int = 50
    homing_speed_z: int = 100
    homing_accel_x: int = 50
    homing_accel_y: int = 50
    homing_accel_z: int = 100

@dataclass
class CoordinateOrigin:
    machine_origin_steps: Dict[str, int] = None
    work_origin_steps: Dict[str, int] = None
    is_homed: bool = False

    def __post_init__(self):
        if self.machine_origin_steps is None:
            self.machine_origin_steps = {"x": 0, "y": 0, "z": 0}
        if self.work_origin_steps is None:
            self.work_origin_steps = {"x": 0, "y": 0, "z": 0}


class CoordinateSystemError(Exception):
    pass


class SharedXYZController:
    """XYZ controller using the shared bus and Modbus helper."""

    def __init__(self, bus: SharedRS485Bus, cfg: Optional[MachineConfig] = None):
        self.bus = bus
        self.mb = XYZModbus(bus)
        self.cfg = cfg or MachineConfig()
        self.origin = CoordinateOrigin()
        self.addr = {MotorAxis.X: 1, MotorAxis.Y: 2, MotorAxis.Z: 3}  # keep 1/2/3

    def mm_to_steps(self, axis: MotorAxis, mm: float) -> int:
        """
        功能: 将指定轴的毫米距离换算成步进电机步数
        参数:
            axis (MotorAxis): 目标电机轴
            mm (float): 待转换的毫米距离
        返回值:
            int: 对应的步进电机步数
        """
        if axis == MotorAxis.X:
            return int(mm * self.cfg.steps_per_mm_x)
        if axis == MotorAxis.Y:
            return int(mm * self.cfg.steps_per_mm_y)
        if axis == MotorAxis.Z:
            return int(mm * self.cfg.steps_per_mm_z)
        raise ValueError(axis)

    def steps_to_mm(self, axis: MotorAxis, steps: int) -> float:
        """
        功能: 将指定轴的步进电机步数换算成毫米距离
        参数:
            axis (MotorAxis): 目标电机轴
            steps (int): 待转换的步进电机步数
        返回值:
            float: 对应的毫米距离
        """
        if axis == MotorAxis.X:
            return steps / self.cfg.steps_per_mm_x
        if axis == MotorAxis.Y:
            return steps / self.cfg.steps_per_mm_y
        if axis == MotorAxis.Z:
            return steps / self.cfg.steps_per_mm_z
        raise ValueError(axis)

    def enable(self, axis: MotorAxis, on: bool = True) -> bool:
        """
        功能: 按需开启或关闭指定轴的使能状态并写入驱动寄存器
        参数:
            axis (MotorAxis): 目标电机轴
            on (bool): 是否开启使能, 默认 True
        返回值:
            bool: 写寄存器是否成功
        """
        return self.mb.write_reg(self.addr[axis], XYZModbus.REG_ENABLE, 0x0001 if on else 0x0000)

    def emergency_stop(self, axis: MotorAxis) -> bool:
        """
        功能: 对指定轴执行急停并写入驱动急停寄存器
        参数:
            axis (MotorAxis): 目标电机轴
        返回值:
            bool: 写寄存器是否成功
        """
        return self.mb.write_reg(self.addr[axis], XYZModbus.REG_EMERGENCY_STOP, 0x0000)

    def get_motor_only_status(self, axis: MotorAxis) -> MotorPosition:
        a = self.addr[axis]
        v = self.mb.read_regs(a, XYZModbus.REG_STATUS, 1)
        return MotorPosition(steps=0, speed=0, current=0, status=MotorStatus(v[0]))
    
    def get_motor_status(self, axis: MotorAxis) -> MotorPosition:
        """
        功能: 读取指定轴的状态寄存器并返回位置、速度、电流及状态
        参数:
            axis (MotorAxis): 目标电机轴
        返回值:
            MotorPosition: 包含位置、速度、电流和状态的聚合结果
        """
        a = self.addr[axis]
        v = self.mb.read_regs(a, XYZModbus.REG_STATUS, 6) # 批量读取状态相关寄存器
        status = MotorStatus(v[0]) # 解析电机状态
        pos = (v[1] << 16) | v[2] # 合并高低位得到位置
        if pos > 0x7FFFFFFF:
            pos -= 0x100000000  # 转换为有符号位置值
        speed = v[3]   # 读取速度
        current = v[5]   # 读取电流
        return MotorPosition(pos, speed, current, status)  # 返回聚合后的电机状态

    def move_to_steps(self, axis: MotorAxis, steps: int, speed_rpm: int = 1000,
                      accel: int = 1000, precision: int = 100) -> bool:
        """
        功能: 以步数形式设置目标位置并配置运动参数
        参数:
            axis (MotorAxis): 目标电机轴
            steps (int): 目标位置步数, 可为负值
            speed_rpm (int): 目标转速, 默认 1000
            accel (int): 加速度, 默认 1000
            precision (int): 精度阈值, 默认 100
        返回值:
            bool: 写寄存器是否成功
        """

        axis_addr = self.addr[axis]
        if steps < 0:
            steps = (steps + 0x100000000) & 0xFFFFFFFF   # 负值转换为无符号步数
        high_word = (steps >> 16) & 0xFFFF   # 提取高16位步数
        low_word = steps & 0xFFFF   # 提取低16位步数
        reserved_word = 0x0000  # 协议保留位
        ok = self.mb.write_regs(axis_addr, XYZModbus.REG_TARGET_POSITION_HIGH, [
            high_word , low_word, reserved_word, speed_rpm, accel, precision
        ])  # 批量写入目标位置和运动参数
        return ok

    def wait_for_completion(self, axis: MotorAxis, timeout: float = 20.0) -> bool:
        """
        等待指定电机轴进入待机状态,期间通过轮询Modbus状态
        参数:
            axis: 目标电机轴枚举,用于查询状态
            timeout: 最大等待秒数,超时后返回False
        返回值:
            bool: 待机则True,超时未待机则False
        异常:
            ModbusException: 连续通信异常达到阈值时抛出
        """

        t0 = time.time()  # 记录起始时间用于超时判断
        misses = 0  # 累计连续通信异常次数
        while time.time() - t0 < timeout:   # 在超时时间内持续轮询
            try: 
                st = self.get_motor_only_status(axis)  # 读取当前轴状态
                misses = 0   # 通信成功则清零异常计数
                if st.status == MotorStatus.STANDBY:  # 达到待机状态则完成
                    return True
            except ModbusException:   # 累加通信异常用于阈值判断
                misses += 1
                if misses >= 10:   # 连续异常过多则抛出异常中断
                    raise
            time.sleep(0.05)   # 短暂休眠避免高频占用
        return False  # 超时未进入待机状态

    def get_homing_speed(self, axis: MotorAxis) -> int:
        """
        获取指定电机轴的归零速度,优先返回轴专属配置,缺省时回退到全局默认
        参数:
            axis: 目标电机轴枚举,用于选择对应的归零速度
        返回值:
            int: 归零速度值,单位与配置一致
        """

        if axis == MotorAxis.X and self.cfg.homing_speed_x is not None:
            return self.cfg.homing_speed_x
        if axis == MotorAxis.Y and self.cfg.homing_speed_y is not None:
            return self.cfg.homing_speed_y
        if axis == MotorAxis.Z and self.cfg.homing_speed_z is not None:
            return self.cfg.homing_speed_z
        return self.cfg.homing_speed  # 无专属配置时回退全局默认

    def get_homing_accel(self, axis: MotorAxis) -> int:
        """
        获取指定电机轴的归零加速度,优先使用轴级配置,若缺省则回退默认值
        参数:
            axis: 目标电机轴枚举,用于选择对应的归零加速度
        返回值:
            int: 归零加速度数值,单位与配置一致
        """

        if axis == MotorAxis.X and self.cfg.homing_accel_x is not None:
            return self.cfg.homing_accel_x
        if axis == MotorAxis.Y and self.cfg.homing_accel_y is not None:
            return self.cfg.homing_accel_y
        if axis == MotorAxis.Z and self.cfg.homing_accel_z is not None:
            return self.cfg.homing_accel_z
        return self.cfg.homing_accel

    def home_axis(self, axis: MotorAxis, direction: int = -1) -> bool:
        """
        归零指定电机轴: 上电使能轴, 设置归零速度与加速度, 运行到限位或位置稳定后急停, 再退回安全距离并记录原点
        参数:
            axis: 目标电机轴枚举, 用于选择对应的地址和配置
            direction: 归零方向, -1为负向, 1为正向
        返回值:
            bool: 归零流程完成则True, 写寄存器失败则False
        异常:
            ModbusException: 归零过程中通信异常将透传
        """

        a = self.addr[axis]
        self.enable(axis, True)   # 确保轴上电并使能
        speed = self.get_homing_speed(axis) * direction   # 按方向设置归零速度
        accel = self.get_homing_accel(axis)  # 获取轴级或默认归零加速度

        # if not self.mb.write_reg(a, XYZModbus.REG_SPEED_MODE_ACCELERATION, accel & 0xFFFF):
        #     return False
        # if not self.mb.write_reg(a, XYZModbus.REG_SPEED_MODE_SPEED, speed & 0xFFFF):
        #     return False
        
        # 写入速度模式的速度和加速度
        if not self.mb.write_regs(a, XYZModbus.REG_SPEED_MODE_SPEED, [speed & 0xFFFF, accel & 0xFFFF]):
            return False

        last = None
        stable_since = None
        t0 = time.time()
        while time.time() - t0 < self.cfg.homing_timeout:  # 在归零超时前循环监测
            st = self.get_motor_status(axis)  # 轮询当前轴状态
            pos = st.steps
            if (direction < 0 and st.status == MotorStatus.REVERSE_LIMIT_STOP) or \
               (direction > 0 and st.status == MotorStatus.FORWARD_LIMIT_STOP):
                self.emergency_stop(axis)  # 命中对应限位后立即急停
                final = pos
                break
            if last is not None:
                if abs(pos - last) <= 1:
                    stable_since = stable_since or time.time()   # 记录首次稳定时间
                    if time.time() - stable_since >= self.cfg.position_stable_time:
                        self.emergency_stop(axis)  # 长时间无位移判定为到位急停
                        final = pos
                        break
                else:
                    stable_since = None  # 位置变化则重置稳定计时
            last = pos  # 记住本次位置以便下次对比
            time.sleep(self.cfg.position_check_interval)   # 节流轮询频率
        else:
            self.emergency_stop(axis)
            final = self.get_motor_status(axis).steps

        clear_steps = self.mm_to_steps(axis, self.cfg.safe_clearance)  # 计算安全退让步数
        safe_pos = final + (-direction) * clear_steps    #退回限位方向的反向安全距离
        self.move_to_steps(axis, safe_pos, self.cfg.default_speed, self.cfg.default_acceleration) # 退让到安全位置
        self.wait_for_completion(axis, 10.0) # 等待退让动作完成
        self.origin.machine_origin_steps[axis.name.lower()] = final  # 记录归零时的原点步数
        return True

    def home_all(self) -> bool:
        """
        功能: 依次对 Z、X、Y 轴执行回零并设置当前点为工作坐标原点
        参数: 无
        返回: bool, 全轴回零和原点设置成功返回 True, 否则 False
        """
        for ax in (MotorAxis.Z, MotorAxis.X, MotorAxis.Y):
            if not self.home_axis(ax, -1):
                return False
            time.sleep(0.3)
        self.origin.is_homed = True 
        try: 
            self.set_work_origin_here() # 回零后将当前位置记录为工作系原点
        except Exception:
            # 若失败不影响 homing 结果，只在需要时手动再设。
            pass
        return True

    def set_work_origin_here(self) -> bool:
        """
        功能: 读取当前各轴步进值并将其记录为工作坐标原点
        参数: 无
        返回: bool, 设置成功返回 True
        """
        pos = {
            'x': self.get_motor_status(MotorAxis.X).steps,
            'y': self.get_motor_status(MotorAxis.Y).steps,
            'z': self.get_motor_status(MotorAxis.Z).steps,
        }  # 抓取 X/Y/Z 轴当前步数
        self.origin.work_origin_steps = pos # 保存为工作系原点步进值
        return True

    def work_to_machine_steps(self, x=None, y=None, z=None) -> Dict[str, int]:
        """
        功能: 
            将工作坐标系的毫米位移转换为机床步进值并返回合并结果
        参数: 
            x/y/z 可选浮点, 表示工作系位移(mm), None 表示忽略该轴
        返回: 
            Dict[str, int], 包含传入轴对应的机床步进值
        """
        out = {}
        if x is not None:
            out['x'] = self.origin.work_origin_steps['x'] + self.mm_to_steps(MotorAxis.X, x)
        if y is not None:
            out['y'] = self.origin.work_origin_steps['y'] + self.mm_to_steps(MotorAxis.Y, y)
        if z is not None:
            out['z'] = self.origin.work_origin_steps['z'] + self.mm_to_steps(MotorAxis.Z, z)
        return out

    def check_limits(self, x=None, y=None, z=None):
        """
        检查传入坐标是否超出配置的最大行程范围, 仅校验提供的轴
        协议: 坐标超出范围时抛出 CoordinateSystemError, 合法时无返回
        参数:
            x_pos: float 可选, X 轴坐标
            y_pos: float 可选, Y 轴坐标
            z_pos: float 可选, Z 轴坐标
        返回值:
            None: 所有已提供坐标均在范围内时
        """
        if x is not None and (x < 0 or x > self.cfg.max_travel_x):
            raise CoordinateSystemError(f"X out of range: {x}")
        if y is not None and (y < 0 or y > self.cfg.max_travel_y):
            raise CoordinateSystemError(f"Y out of range: {y}")
        if z is not None and (z < 0 or z > self.cfg.max_travel_z):
            raise CoordinateSystemError(f"Z out of range: {z}")
   
    def move_to_work_safe(self, x=None, y=None, z=None, speed=None, accel=None) -> bool:
        """
        功能:
            在工作坐标系下执行安全移动: 先抬Z轴到安全高度, 再移动XY, 最后下降到目标Z, 并临时忽略CRC异常
        参数:
            x 可选浮点, 目标X坐标(毫米), None表示不移动X
            y 可选浮点, 目标Y坐标(毫米), None表示不移动Y
            z 可选浮点, 目标Z坐标(毫米), None表示不移动Z
            speed 可选整数, 运动速度(步频), None使用默认速度
            accel 可选整数, 运动加速度(步频变化率), None使用默认加速度
        返回:
            布尔值, True表示安全移动流程顺利完成, False表示过程中出现异常
        """

        # 限位与默认参数
        self.check_limits(x, y, z)
        speed = speed or self.cfg.default_speed
        accel = accel or self.cfg.default_acceleration

        # 临时开启“忽略 CRC 错误”
        prev_ignore = getattr(self.mb, "ignore_crc_error", False)
        try:
            self.mb.set_ignore_crc(True)

            # 先抬到安全 Z（若给了 z 目标）
            if z is not None:
                safe_steps = self.work_to_machine_steps(z=self.cfg.safe_z_height)['z']
                self.move_to_steps(MotorAxis.Z, safe_steps, speed + 100 , accel + 500)
                self.wait_for_completion(MotorAxis.Z, 20.0)

            # 下发 XY 目标
            if x is not None:
                self.move_to_steps(MotorAxis.X, self.work_to_machine_steps(x=x)['x'], speed, accel)
                
            if y is not None:
                self.move_to_steps(MotorAxis.Y, self.work_to_machine_steps(y=y)['y'], speed, accel)

            # 等待 XY 完成
            if x is not None:
                self.wait_for_completion(MotorAxis.X, 20.0)
            if y is not None:
                self.wait_for_completion(MotorAxis.Y, 20.0)

            # 最后降到目标 Z
            if z is not None:
                self.move_to_steps(MotorAxis.Z, self.work_to_machine_steps(z=z)['z'], speed + 100 , accel + 500)
                self.wait_for_completion(MotorAxis.Z, 20.0)

            return True
        finally:
            # 恢复之前的 CRC 忽略开关
            try:
                self.mb.set_ignore_crc(prev_ignore)
            except Exception:
                pass

    def move_relative_direct(self, dx: float, dy: float, dz: float,
                            speed: Optional[int]=None, accel: Optional[int]=None,
                            z_order: str="auto") -> bool:
        """
        功能:
            基于当前位置按工作系毫米量执行直接相对位移, 不抬Z, 按z_order决定Z与XY顺序
        参数:
            dx 浮点数, 工作系X相对位移(mm)
            dy 浮点数, 工作系Y相对位移(mm)
            dz 浮点数, 工作系Z相对位移(mm)
            speed 可选整数, 运动速度(步频), None使用默认
            accel 可选整数, 运动加速度(步频变化率), None使用默认
            z_order 字符串, Z执行顺序(first/last/auto)
        返回:
            布尔值, True表示在超时前完成移动, False表示异常或未完成
        """
        speed = speed if speed is not None else self.cfg.default_speed
        accel = accel if accel is not None else self.cfg.default_acceleration
        # 读取当前步进并计算目标步进
        cur_x = self.get_motor_status(MotorAxis.X).steps
        cur_y = self.get_motor_status(MotorAxis.Y).steps
        cur_z = self.get_motor_status(MotorAxis.Z).steps
        tgt_x = cur_x + self.mm_to_steps(MotorAxis.X, dx)
        tgt_y = cur_y + self.mm_to_steps(MotorAxis.Y, dy)
        tgt_z = cur_z + self.mm_to_steps(MotorAxis.Z, dz)
        # 根据z_order决定Z与XY执行顺序
        if z_order == "first":
            order = ("z", "xy")
        elif z_order == "last":
            order = ("xy", "z")
        else:
            order = ("xy", "z") if tgt_z > cur_z else ("z", "xy")
        ok = True
        try:
            if order[0] == "z":
                ok &= self.move_to_steps(MotorAxis.Z, tgt_z, speed, accel)
                ok &= self.wait_for_completion(MotorAxis.Z, 20.0)
                ok &= self.move_to_steps(MotorAxis.X, tgt_x, speed, accel)
                ok &= self.move_to_steps(MotorAxis.Y, tgt_y, speed, accel)
                ok &= self.wait_for_completion(MotorAxis.X, 20.0)
                ok &= self.wait_for_completion(MotorAxis.Y, 20.0)
            else:
                ok &= self.move_to_steps(MotorAxis.X, tgt_x, speed, accel)
                ok &= self.move_to_steps(MotorAxis.Y, tgt_y, speed, accel)
                ok &= self.wait_for_completion(MotorAxis.X, 20.0)
                ok &= self.wait_for_completion(MotorAxis.Y, 20.0)
                ok &= self.move_to_steps(MotorAxis.Z, tgt_z, speed, accel)
                ok &= self.wait_for_completion(MotorAxis.Z, 20.0)
        except Exception:
            ok = False
        return bool(ok)
    
    def move_to_work_direct(self, x=None, y=None, z=None,
                        speed=None, accel=None, z_order: str="auto") -> bool:
        """
        功能:
            在工作坐标系下直接移动到给定目标, 不抬Z, 按z_order决定Z与XY顺序
        参数:
            x/y/z 可选浮点, 目标工作系坐标(mm), None表示忽略该轴
            speed 可选整数, 运动速度(步频), None使用默认
            accel 可选整数, 运动加速度(步频变化率), None使用默认
            z_order 字符串, Z执行顺序(first/last/auto)
        返回:
            布尔值, True表示在超时前完成移动, False表示异常或未完成
        """
        self.check_limits(x, y, z)
        speed = speed if speed is not None else self.cfg.default_speed
        accel = accel if accel is not None else self.cfg.default_acceleration
        cur = self.get_work_position_mm()
        # 计算相对位移以复用直接移动策略
        dx = (x - cur["x"]) if x is not None else 0.0
        dy = (y - cur["y"]) if y is not None else 0.0
        dz = (z - cur["z"]) if z is not None else 0.0
        return self.move_relative_direct(dx, dy, dz, speed=speed, accel=accel, z_order=z_order)

    def move_rel_z_mm(self, dz: float, speed=1000, accel=1000) -> bool:
        cur = self.get_motor_status(MotorAxis.Z).steps
        tgt = cur + self.mm_to_steps(MotorAxis.Z, dz)
        self.move_to_steps(MotorAxis.Z, tgt, speed, accel, 50)
        return self.wait_for_completion(MotorAxis.Z, 10.0)
    
    def machine_steps_to_work_mm(self, x=None, y=None, z=None):
        """
        功能:
            将机器坐标系的步进值转换为工作坐标系下的毫米位移, 按需返回各轴结果
        参数:
            x 可选整数或可转整数, 机床X轴步进值, None表示忽略X
            y 可选整数或可转整数, 机床Y轴步进值, None表示忽略Y
            z 可选整数或可转整数, 机床Z轴步进值, None表示忽略Z
        返回:
            Dict[str, float], 传入轴对应的工作系毫米位移
        """
        out = {}
        if x is not None:
            dx = int(x) - int(self.origin.work_origin_steps['x'])
            out['x'] = self.steps_to_mm(MotorAxis.X, dx)
        if y is not None:
            dy = int(y) - int(self.origin.work_origin_steps['y'])
            out['y'] = self.steps_to_mm(MotorAxis.Y, dy)
        if z is not None:
            dz = int(z) - int(self.origin.work_origin_steps['z'])
            out['z'] = self.steps_to_mm(MotorAxis.Z, dz)
        return out

    def machine_to_work_mm(self, x=None, y=None, z=None): 
        """
        功能:
            将机器坐标系的毫米位置转换为工作坐标系下的毫米位置, 返回传入轴的偏移结果
        参数:
            x 可选浮点, 机床X轴位置(毫米), None表示忽略X
            y 可选浮点, 机床Y轴位置(毫米), None表示忽略Y
            z 可选浮点, 机床Z轴位置(毫米), None表示忽略Z
        返回:
            Dict[str, float], 传入轴对应的工作系毫米位置
        """
        out = {}
        if x is not None:
            xs = self.mm_to_steps(MotorAxis.X, x) - self.origin.work_origin_steps['x']
            out['x'] = self.steps_to_mm(MotorAxis.X, xs)
        if y is not None:
            ys = self.mm_to_steps(MotorAxis.Y, y) - self.origin.work_origin_steps['y']
            out['y'] = self.steps_to_mm(MotorAxis.Y, ys)
        if z is not None:
            zs = self.mm_to_steps(MotorAxis.Z, z) - self.origin.work_origin_steps['z']
            out['z'] = self.steps_to_mm(MotorAxis.Z, zs)
        return out

    def get_work_position_mm(self):
        """
        功能:
            读取当前机器步进状态并转换为工作坐标系下的毫米位置
        参数:
            无
        返回:
            Dict[str, float], 当前XYZ在工作坐标系中的毫米位置
        """
        sx = self.get_motor_status(MotorAxis.X).steps
        sy = self.get_motor_status(MotorAxis.Y).steps
        sz = self.get_motor_status(MotorAxis.Z).steps
        return self.machine_steps_to_work_mm(x=sx, y=sy, z=sz)