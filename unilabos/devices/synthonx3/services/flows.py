from __future__ import annotations
import time
import re
from dataclasses import dataclass
from typing import Dict, List, Iterable, Optional, Tuple, Union
from contextlib import contextmanager
from unilabos.devices.synthonx3.services.station import Station
from unilabos.devices.synthonx3.drivers.stir import RelayController
import logging
import serial.tools.list_ports as ports

logger = logging.getLogger("liquid_station")

# -------------------- 小工具 --------------------
def _require(cond: bool, msg: str):
    if not cond:
        raise ValueError(msg)

def _zone_from_name(name: str) -> str:
    """返回点位所属分区的字母（A/B/C/D）"""
    for c in name:
        if c.isalpha():
            return c.upper()
    raise ValueError(f"无法解析点名分区: {name!r}")

def _split_names(names: Union[str, Iterable[str]]) -> List[str]:
    if isinstance(names, str):
        return [p.strip() for p in names.split(",") if p.strip()]
    return [str(p).strip() for p in names]

# -------------------- 配置与主类 --------------------
@dataclass
class Flow:
    """
    提供高层流程 API；内部统一用 Station：
      - 点到点：station.move_to(x, y, z)
      - 相对位移/下探/上升：station.move_to_work_direct(dx, dy, dz)
      - 移液：station.pipette_init / eject_tip / aspirate / dispense
    """
    def __init__(self, 
                 station: Optional[Station] = None, 
                 port: str = "COM50", 
                 baudrate: int = 115200, 
                 points_file: str = "points.json",
                 **kwargs):
        
        # 1. 判断初始化方式
        if station is not None:
            # 方式 A: 外部代码手动传入了 station 对象
            self.station = station
        else:
            # 方式 B: 系统通过 YAML 启动，传入了 port 等参数
            print(f"Flow 初始化: 正在连接 Station (Port={port}, Baud={baudrate}, Points={points_file})...")
            self.station = Station(port=port, baudrate=int(baudrate), points_file=points_file)
            
            # 尝试连接，如果连接失败可以抛出异常或打印错误
            try:
                self.station.connect()
            except Exception as e:
                logger.error(f"Station 连接失败: {e}")
                raise e
        
        # 2. 配置其他参数 (优先使用传入的 kwargs，否则使用默认值)
        self.settle_s = kwargs.get('settle_s', 0.5)          # 动作后的静置时间
        self.approach_lift = kwargs.get('approach_lift', 0.0)     # 接近时的抬升
        
        self.pick_down_mm = 120.0    # 取枪头下探深度
        self.drop_down_mm = 60.0     # 丢枪头下探深度
        
        # 移液后等待时间
        self.delay_after_aspirate = kwargs.get('delay_after_aspirate', 0.35)
        self.delay_after_dispense = kwargs.get('delay_after_dispense', 0.35)
    
    # =======================================================
    # 1) 系统初始化
    # =======================================================
    def system_init(self) -> bool:
        print("系统初始化：全轴回零...")
        ok = self.station.home_all()
        print("系统已回零")
        ok2 = self.station.set_work_origin_here()
        print("设置当前位置设为工作原点")
        return bool(ok and ok2)

    # =======================================================
    # 2) 取枪头 / 放枪头
    # =======================================================
    def pick_tip(self, tip_point: str, down_mm: float = 120) -> bool:
        """取枪头"""
        logger.info(f"Picking tip at {tip_point}")
        x, y, z = self.station.get_point(tip_point)
        # 原代码逻辑：直接移动到 z + down_mm
        target_z = z + down_mm
        self.station.move_to(x, y, target_z)
        time.sleep(self.settle_s)
        logger.info(f"{tip_point} 枪头已装载")
        return True
    
    def drop_tip(self, tip_point: str, down_mm: float = 60) -> bool:
        """丢枪头"""
        logger.info(f"Dropping tip at {tip_point}")
        x, y, z = self.station.get_point(tip_point)
        target_z = z + down_mm
        
        self.station.move_to(x, y, target_z)
        time.sleep(self.settle_s)
        self.station.pip.eject_tip()
        time.sleep(self.settle_s)
        logger.info(f"枪头已弃置在 {tip_point}")
        return True
    
    # =======================================================
    # 3) 转移液体
    # =======================================================
    def transfer(self,
                    src_name: str = "",
                    dst_names: Union[str, Iterable[str]] = "", # 给默认值，防止UI没传时报错
                    tip_c_name: str = "",
                    total_ul: float = 0.0,
                    down_src_mm: float = 120.0, # 给默认下探深度 (参考 pick_tip 的 120)
                    down_dst_mm: float = 60.0,  # 给默认下探深度 (参考 drop_tip 的 60)
                    split_volumes: Optional[List[float]] = None,
                    stir_post_s: Optional[float] = None) -> bool:

        if not src_name or not dst_names or not tip_c_name or total_ul <= 0:
            logger.warning(f"Transfer 调用参数不足或无效: src={src_name}, dst={dst_names}, tip={tip_c_name}, vol={total_ul}")
            return False
        
        # (1) 取枪头
        self.pick_tip(tip_c_name, down_mm=120)
        time.sleep(self.settle_s)

        # (2) 到源位
        dx, dy, dz = self.station.get_point(src_name)
        self.station.move_to(dx, dy, dz + down_src_mm)
        time.sleep(self.settle_s)
        print('到达源点位')

        # (3) 下探源位
        # self.station.move_to_work_direct(0.0, 0.0, float(down_src_mm))
        # time.sleep(self.settle_s)
        # print('下探完成')

        # (4) 吸液
        self.station.pip.aspirate(float(total_ul))
        time.sleep(self.settle_s)
        print('吸取液体完成')

        # (5) 回升源位
        # self.station.move_to_work_direct(0.0, 0.0, -float(down_src_mm))
        # time.sleep(self.settle_s)
        # print('回升完成')

        # === 目标处理 ===
        dst_list = _split_names(dst_names)
        _require(len(dst_list) >= 1, "目标点名至少1个")
        if split_volumes:
            _require(len(split_volumes) == len(dst_list), "split_volumes 长度不匹配")
            vols = [float(v) for v in split_volumes]
        else:
            # 均分模式
            each = float(total_ul) / len(dst_list)
            vols = [each] * len(dst_list)
        
        for i, (dst_name, vol) in enumerate(zip(dst_list, vols)):
            dx, dy, dz = self.station.get_point(dst_name)
            self.station.move_to(dx, dy, dz + down_dst_mm)
            time.sleep(self.settle_s)
            logger.info(f"到达目标 {dst_name}，排液 {vol}uL")
            self.station.pip.dispense(float(vol))
            time.sleep(self.settle_s)

        # —— 如果设定了加液后搅拌时间，则触发搅拌 ——
        if stir_post_s is not None and float(stir_post_s) > 0:
            try:
                print(f"[搅拌] 加液完成，搅拌 {float(stir_post_s)} s ...")
                self.stir_for(float(stir_post_s))
            except Exception as e:
                print(f"[搅拌] 触发失败：{e}（忽略，不影响主流程）")

        # 映射 C 槽到 +48 的弃置位
        def upgrade_c_name(name: str) -> str:
            m = re.fullmatch(r"C(\d+)", name.strip().upper())
            if not m:
                return name
            idx = int(m.group(1))
            return f"C{idx + 48}"
        tip_c_name_new = upgrade_c_name(tip_c_name)

        # (10) 放枪头
        self.drop_tip(tip_c_name_new, down_mm=60.0)
        return True
    
    # =======================================================
    # 4) 过滤
    # =======================================================
    def filtering(self,
                  pusher_name: str,      # D9-D16
                  filter_name: str,      # D1-D8
                  down_pick_mm: float,
                  down_filter_mm: float) -> bool:
        dx, dy, dz = self.station.get_point(pusher_name)
        self.station.move_to(dx, dy, dz + down_pick_mm)
        time.sleep(self.settle_s)
        print("到达推杆点位")

        dx, dy, dz = self.station.get_point(filter_name)
        self.station.move_to(dx, dy, dz)
        time.sleep(self.settle_s)
        print("到达过滤点位")

        self.station.move_to_work_direct(0.0, 0.0, float(down_filter_mm))
        time.sleep(self.settle_s)
        print("下压过滤完成")

        # self.station.move_to_work_direct(0.0, 0.0, -20.0)
        # time.sleep(self.settle_s)
        # print("吸取空气完成")

        # self.station.move_to_work_direct(0.0, 0.0, 20.0)
        # time.sleep(self.settle_s)
        # print("排除剩余液体完成")

        self.station.move_to_work_direct(0.0, 0.0, -10.0)
        time.sleep(self.settle_s)

        self.station.pip.eject_tip()
        time.sleep(self.settle_s)
        print("推杆已弹出")

        # self.station.move_to_work_direct(0.0, 0.0, -50.0)
        # time.sleep(self.settle_s)

        print("过滤流程完成")
        return True

    # =======================================================
    # 5) 推动（D25）
    # =======================================================
    def pushing(self,
                tip_c_name: str) -> bool:
        z_down_mm = 136.0
        y_forward_mm = 60.0
        _require(z_down_mm > 0, "z_down_mm 必须>0")
        _require(y_forward_mm > 0, "y_forward_mm 必须>0")
        _require(_zone_from_name(tip_c_name) in ("C"), "枪头点名应在C区")

        self.pick_tip(tip_c_name)

        dx, dy, dz = self.station.get_point("D25")
        self.station.move_to(dx, dy, dz)        
        time.sleep(self.settle_s)
        print("到达 D25 点位")

        self.station.move_to_work_direct(0.0, 0.0, float(z_down_mm))
        time.sleep(self.settle_s)
        print("下压完成")

        self.station.move_to_work_direct(0.0, float(y_forward_mm), 0.0)
        time.sleep(self.settle_s)
        print("推动完成")

        self.station.move_to_work_direct(0.0, 0.0, -float(z_down_mm))
        time.sleep(self.settle_s)
        print("抬升完成")

        def upgrade_c_name(name: str) -> str:
            m = re.fullmatch(r"C(\d+)", name.strip().upper())
            if not m:
                return name
            idx = int(m.group(1))
            return f"C{idx + 48}"
        tip_c_name_new = upgrade_c_name(tip_c_name)
        self.drop_tip(tip_c_name_new)
        print("推动流程完成")

        return True

    # =======================================================
    # 6) 装核磁（单独函数）
    # =======================================================
    def load_for_nmr(self,
                    src_d_name: str,     # 源：D区 (如 D1-D8)
                    dst_d_name: str,     # 目标：D区 (如 D17-D24)
                    tip_c_name: str,     # 枪头：C区
                    total_ul: float,
                    stir_post_s: Optional[float] = None) -> bool:
        down_src_mm = 138
        down_dst_mm = 9
        _require(_zone_from_name(src_d_name) == "D", "源位必须在 D 区")
        _require(_zone_from_name(dst_d_name) == "D", "目标位必须在 D 区")
        _require(src_d_name != dst_d_name, "源与目标不能相同")
        _require(_zone_from_name(tip_c_name) == "C", "枪头点名必须在 C 区")

        return self.transfer(
            src_name=src_d_name,
            dst_names=dst_d_name,
            tip_c_name=tip_c_name,
            total_ul=float(total_ul),
            down_src_mm=float(down_src_mm),
            down_dst_mm=float(down_dst_mm),
            split_volumes=None,
            stir_post_s=stir_post_s
        )

    def run_full_sequence(self):

        logger.info("=== Starting Full Sequence ===")
        # 初始化
        logger.info("System Init")
        self.system_init()

        # 3. A -> B
        logger.info("Transfer A1 -> B1")
        # self.transfer("A1","B1", "C1", 300.0, down_src_mm=121.0, down_dst_mm=26.0)

        # 4. B -> D
        logger.info("Transfer B1 -> D1")
        # self.transfer("B1", ["D1"], "C2", 300.0, down_src_mm=46.0, down_dst_mm=6.0)

        # 5. 过滤
        logger.info("Filtering (D9->D1)")
        # 确保 D9, D1 点位存在
        # self.filtering("D9", "D1", down_pick_mm=117.0, down_filter_mm=73.0)

        # 6. 推动
        logger.info("Pushing")
        # self.pushing("C3")

        # 7. 装核磁
        logger.info("Load NMR (D1->D17)")
        self.load_for_nmr("D1", "D17", "C4", 300.0)

        logger.info("=== Full Sequence Complete ===")
