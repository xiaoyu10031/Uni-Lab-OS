import os
import time
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Iterable, Optional, Tuple, Union
from unilabos.devices.synthonx3.drivers.bus_rs485 import SharedRS485Bus
from unilabos.devices.synthonx3.controllers.xyz_controller import SharedXYZController, MotorAxis, MachineConfig
from unilabos.devices.synthonx3.drivers.pipette import SOPAPipetteYYQ
import json

logger = logging.getLogger("liquid_station")

# ======================== Liquid Station (ALL) ========================
@dataclass
class LiquidParams:
    delay_after_aspirate: float = 0.5
    delay_after_dispense: float = 0.5

class Station:
    """Bring XYZ and Pipette together on one port, with a CLI."""

    def __init__(self, port: str, baudrate: int = 115200, points_file: str = "points.json"):
        self.port = port
        self.baudrate = baudrate
        self.points_file = points_file
        self.points: Dict[str, Dict[str, float]] = {}
        
        # 硬件组件占位
        self.bus: Optional[SharedRS485Bus] = SharedRS485Bus(self.port, self.baudrate)
        self.xyz: Optional[SharedXYZController] = SharedXYZController(self.bus)
        self.pip: Optional[SOPAPipetteYYQ] = SOPAPipetteYYQ(self.bus)

    def connect(self):
        logger.info(f"Connecting to station on {self.port}...")
        self.bus.open()
        if not self.bus.open():
            raise RuntimeError("Failed to open RS485 bus")
        
        # 初始化控制器
        self.xyz = SharedXYZController(self.bus)
        
        # 初始化移液枪驱动 (地址4)
        pip_driver = SOPAPipetteYYQ(self.bus)
        
        # 加载点位
        self.load_points()
        logger.info("Station connected and ready.")

    def disconnect(self):
        """
        功能:
            关闭共享通信总线并释放底层连接资源
        参数:
            无
        返回:
            None
        """

        self.bus.close()

    # ---- points DB
    def load_points(self):
        """
        功能:
            从配置文件读取点位定义并加载到内存缓存
        参数:
            无
        返回:
            None
        """
        if not os.path.isabs(self.points_file):
            # 获取 station.py 所在的目录 (.../synthonx3/services)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            base_dir = os.path.dirname(current_dir)
            candidate_path = os.path.join(base_dir, self.points_file)
            
            if os.path.exists(candidate_path):
                self.points_file = candidate_path
            else:
                candidate_path_2 = os.path.join(current_dir, self.points_file)
                if os.path.exists(candidate_path_2):
                    self.points_file = candidate_path_2

        logger.info(f"Loading points from: {self.points_file}")

        try:
            with open(self.points_file, "r", encoding="utf-8") as f:
                self.points = json.load(f)
            logger.info(f"Loaded {len(self.points)} points.") # 打印加载成功的点位数
        except Exception as e:
            logger.error(f"Failed to load points file: {e}") 
            self.points = {}
            

    def save_points(self):
        """
        功能:
            将内存中的点位定义写入配置文件, 持久化当前点位
        参数:
            无
        返回:
            None
        """

        with open(self.points_file, "w", encoding="utf-8") as f:
            json.dump(self.points, f, indent=2, ensure_ascii=False)

    def get_point(self, name: str) -> Tuple[float, float, float]:
        """获取某点的 (x, y, z) 坐标，如果不存在则抛出异常"""
        if name not in self.points:
            raise ValueError(f"Point '{name}' not found in DB")
        p = self.points[name]
        return (float(p['x']), float(p['y']), float(p['z']))

    def _idx_from_name(name: str) -> int:
        """提取 'C1' / 'D49' 的编号 -> 1 / 49"""
        try:
            return int(''.join(c for c in name if c.isdigit()))
        except Exception:
            raise ValueError(f"无法解析点名编号: {name!r}")
    
    def set_point_current(self, name: str):
        """将当前机器位置保存为某个点"""
        if not self.xyz: raise RuntimeError("XYZ not initialized")
        pos = self.xyz.get_position_mm()
        self.points[name] = pos
        self.save_points()
        logger.info(f"Point '{name}' updated to {pos}")

    # ---- station ops
    def home_all(self):
        """
        功能:
            触发XYZ控制器执行全轴回零并返回执行结果
        参数:
            无
        返回:
            控制器返回值, True表示回零成功, False或异常表示失败
        """

        if self.xyz: self.xyz.home_all()

    def set_work_origin_here(self):
        """
        功能:
            将当前机床位置设置为新的工作坐标系原点, 并同步到控制器
        参数:
            无
        返回:
            控制器返回值, True表示设置成功, False或异常表示失败
        """

        if self.xyz: self.xyz.set_work_origin_here()

    def move_to(self, x=None, y=None, z=None, speed=None, accel=None):
        """
        功能:
            在工作坐标系下执行安全移动, 将XYZ目标传递给底层安全移动接口
        参数:
            x 可选浮点, 目标X坐标(毫米), None表示不移动X
            y 可选浮点, 目标Y坐标(毫米), None表示不移动Y
            z 可选浮点, 目标Z坐标(毫米), None表示不移动Z
            speed 可选整数, 运动速度(步频), None使用默认速度
            accel 可选整数, 运动加速度(步频变化率), None使用默认加速度
        返回:
            布尔值, True表示安全移动完成, False或异常表示失败
        """

        if self.xyz: self.xyz.move_to_work_safe(x, y, z, speed, accel)
    
    def move_to_direct(self, x=None, y=None, z=None, speed=None, accel=None, z_order: str = "auto"):
        """不抬Z直接移动到工作坐标。z_order可为 first/last/auto"""
        return self.xyz.move_to_work_direct(x=x, y=y, z=z, speed=speed, accel=accel, z_order=z_order)
       
    def move_to_work_direct(self, x=None, y=None, z=None,
                            speed=None, accel=None, z_order: str = "auto"):
        """直达运动：可指定 Z 先/后（z_order=first/last/auto）。"""
        assert self.xyz is not None  # 确保已 connect()
        return self.xyz.move_to_work_direct(
            x=x, y=y, z=z, speed=speed, accel=accel, z_order=z_order
        )
    
    def move_rel_z(self, dz_mm: float):
        return self.xyz.move_rel_z_mm(dz_mm, 1000, 1000)


    def move_to_point(self, name: str, safe_mode: bool = True):
        x, y, z = self.get_point(name)
        if self.xyz:
            self.xyz.move_to_work_safe(x, y, z)


# ================================ CLI =================================
def main():
    print("\n=======SynthonX=======")
    port = input("串口端口 (默认 /dev/ttyUSB1): ").strip() or "ttyUSB1"
    station = Station(port)
    station.connect()
    station.load_points()

    init_pip = input("是否初始化移液器? (y/N): ").strip().lower() in ("y", "yes")
    if init_pip:
        if station.pipette_init():
            print("移液器初始化完成。")
        else:
            print("移液器初始化失败。")

    while True:
        print("\n" + "=" * 50)
        print("1) 全轴回零（Z→X→Y）")
        print("2) 设定当前位置为工作原点")
        print("3) 安全移动到点 (X/Y/Z，mm)")
        print("4) Z 轴相对移动 (mm)")
        print("5) 保存/前往点位")
        print("6) 移液：初始化 / 弹出枪头 / 吸液 / 排液")
        print("7) 直接移动(不抬Z) X/Y/Z + 顺序(first/last/auto)")
        print("99) 紧急停止")
        print("0) 退出")
        choice = input("选择: ").strip()

        if choice == "0":
            break

        elif choice == "1":
            print("回零中…")
            print("成功" if station.home_all() else "失败")

        elif choice == "2":
            print("设定工作原点…")
            print("成功" if station.set_work_origin_here() else "失败")

        elif choice == "3":
            x = input("X(mm, 空=跳过): ").strip()
            y = input("Y(mm, 空=跳过): ").strip()
            z = input("Z(mm, 空=跳过): ").strip()
            x = float(x) if x else None
            y = float(y) if y else None
            z = float(z) if z else None
            ok = station.move_to(x, y, z)
            print("到位" if ok else "失败")

        elif choice == "4":
            dz = float(input("Z 相对位移(mm，正=下降): ").strip())
            ok = station.move_rel_z(dz)
            print("完成" if ok else "失败")

        elif choice == "5":
            sub = input("(a)保存点  (b)前往点: ").strip().lower()
            if sub == "a":
                name = input("点名: ").strip()
                x = float(input("X(mm): ").strip())
                y = float(input("Y(mm): ").strip())
                z = float(input("Z(mm): ").strip())
                station._points[name] = {"x": x, "y": y, "z": z}
                station.save_points()
                print("已保存")
            else:
                name = input("点名: ").strip()
                pt = station._points.get(name)
                if not pt:
                    print("未找到该点")
                else:
                    ok = station.move_to(pt["x"], pt["y"], pt["z"])
                    print("到位" if ok else "失败")

        elif choice == "6":
            sub = input("(a)初始化  (b)弹出枪头  (c)吸液  (d)排液: ").strip().lower()
            if sub == "a":
                print("初始化…")
                print("完成" if station.pipette_init() else "失败")
            elif sub == "b":
                print("弹出枪头…")
                station.eject_tip()
                print("完成")
            elif sub == "c":
                vol = float(input("吸液体积(µL): ").strip())
                station.aspirate(vol)
            elif sub == "d":
                vol = float(input("排液体积(µL): ").strip())
                station.dispense(vol)
            else:
                print("无效子选项")

        elif choice == "7":
            x = input("X(mm, 空=跳过): ").strip()
            y = input("Y(mm, 空=跳过): ").strip()
            z = input("Z(mm, 空=跳过): ").strip()
            z_order = input("Z顺序(first/last/auto, 默认auto): ").strip().lower() or "auto"
            x = float(x) if x else None
            y = float(y) if y else None
            z = float(z) if z else None
            ok = station.move_to_direct(x=x, y=y, z=z, z_order=z_order)
            print("到位" if ok else "失败")

        elif choice == "99":
            station.estop_all()
            print("已急停")

        else:
            print("无效选项")

    station.disconnect()
    print("已退出")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    main()
