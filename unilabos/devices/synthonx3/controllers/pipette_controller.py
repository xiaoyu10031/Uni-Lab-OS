import time
import logging
from dataclasses import dataclass
from enum import Enum
from unilabos.devices.synthonx3.drivers.pipette import PipetteDriver, SOPAConfig

logger = logging.getLogger("controller.pipette")

# 1. 定义液体类型
class LiquidClass(Enum):
    WATER = "water"       # 水：标准速度
    VISCOUS = "viscous"   # 粘性：慢速，长延时
    VOLATILE = "volatile" # 挥发性：快速，防滴漏

# 2. 定义液体处理参数
@dataclass
class LiquidParams:
    aspirate_speed: int    # 吸液速度 (对应 s 指令)
    dispense_speed: int    # 排液速度
    blow_out_volume: float # 吹出体积 (uL)
    settle_delay: float    # 动作后静置时间 (s)
    air_gap: float         # 空气隔离段 (uL)
    pre_wet: bool = False  # 是否预润湿

class PipetteController:
    """
    移液高级控制器。
    职责：
    1. 状态管理 (当前体积、枪头状态)
    2. 策略应用 (根据液体类型自动调整硬件参数)
    3. 动作序列 (吸液=变通过速+吸+停顿+吸空气)
    """

    # 预定义策略库
    # 假设硬件 1 unit = 1 uL (具体需根据硬件手册调整)
    LIQUID_CONFIGS = {
        LiquidClass.WATER: LiquidParams(
            aspirate_speed=2000, dispense_speed=3000, 
            blow_out_volume=10, settle_delay=0.2, air_gap=5, pre_wet=False
        ),
        LiquidClass.VISCOUS: LiquidParams(
            aspirate_speed=300, dispense_speed=500, # 粘性液体速度很慢
            blow_out_volume=20, settle_delay=1.5, air_gap=10, pre_wet=True
        ),
        LiquidClass.VOLATILE: LiquidParams(
            aspirate_speed=3000, dispense_speed=4000, # 挥发性液体速度快
            blow_out_volume=5, settle_delay=0.1, air_gap=10, pre_wet=True
        )
    }

    def __init__(self, driver: PipetteDriver, max_volume_ul: float = 1000.0):
        self.driver = driver
        self.max_volume = max_volume_ul
        
        # 状态追踪
        self.current_volume = 0.0
        self.has_tip = False
        
        # 默认液体策略
        self.active_params = self.LIQUID_CONFIGS[LiquidClass.WATER]

    def initialize(self):
        """初始化：发送指令并等待硬件复位"""
        logger.info("Controller: Initializing Pipette...")
        self.driver.init_device()
        
        # 硬件归零通常需要几秒钟，这里强制等待
        time.sleep(3.0) 
        
        # 应用默认速度
        self._apply_speed_params(self.active_params)
        
        # 同步状态
        self.current_volume = 0.0
        try:
            self.has_tip = self.driver.get_tip_status()
            logger.info(f"Pipette Ready. Tip Attached: {self.has_tip}")
        except Exception as e:
            logger.warning(f"Could not read tip status: {e}")
            self.has_tip = False

    def set_liquid_class(self, liquid_type: LiquidClass):
        """切换液体模式"""
        if liquid_type in self.LIQUID_CONFIGS:
            self.active_params = self.LIQUID_CONFIGS[liquid_type]
            logger.info(f"Switched to liquid class: {liquid_type.name}")
        else:
            logger.warning(f"Unknown liquid class {liquid_type}")

    def _apply_speed_params(self, params: LiquidParams):
        """将策略参数下发给硬件"""
        self.driver.set_acceleration(2000) # 默认加速度
        self.driver.set_max_speed(params.aspirate_speed)

    def eject_tip(self):
        """智能退枪头"""
        self.driver.eject_tip()
        self.has_tip = False
        self.current_volume = 0.0
        logger.info("Tip ejected.")

    def aspirate(self, volume_ul: float):
        """
        智能吸液流程：检查 -> 调速 -> 预润湿 -> 吸液 -> 静置 -> 空气隔离
        """
        if not self.has_tip:
            logger.warning("Controller: No tip attached! Proceeding carefully...")
        
        if self.current_volume + volume_ul > self.max_volume:
            logger.error(f"Volume overflow! Max: {self.max_volume}, Curr: {self.current_volume}")
            return False

        # 1. 设置吸液速度
        self.driver.set_max_speed(self.active_params.aspirate_speed)

        # 2. 预润湿 (Pre-wet)
        if self.active_params.pre_wet and self.current_volume == 0:
            logger.info("Pre-wetting...")
            wet_vol = min(volume_ul, self.max_volume * 0.5)
            self.driver.move_plunger(int(wet_vol), 'P')
            time.sleep(0.2)
            self.driver.move_plunger(int(wet_vol), 'D')
            time.sleep(0.2)

        # 3. 执行吸液
        vol_int = int(volume_ul)
        self.driver.move_plunger(vol_int, 'P')
        self.current_volume += volume_ul

        # 4. 静置 (Settle) - 等待液体平稳
        if self.active_params.settle_delay > 0:
            time.sleep(self.active_params.settle_delay)

        # 5. 吸空气隔离段 (Air Gap) - 防止移动时滴液
        if self.active_params.air_gap > 0:
            # 慢速吸空气
            gap_speed = int(self.active_params.aspirate_speed / 2)
            self.driver.set_max_speed(gap_speed)
            
            gap_int = int(self.active_params.air_gap)
            self.driver.move_plunger(gap_int, 'P')
            self.current_volume += self.active_params.air_gap
        
        logger.info(f"Aspirated {volume_ul}uL. Current vol: {self.current_volume}")
        return True

    def dispense(self, volume_ul: float, blow_out: bool = False):
        """
        智能排液流程：调速 -> 排液 -> 静置 -> 吹出残液
        """
        # 允许排液量略微超过记录值(处理空气段误差)
        
        # 1. 设置排液速度
        self.driver.set_max_speed(self.active_params.dispense_speed)

        # 2. 执行排液
        vol_int = int(volume_ul)
        self.driver.move_plunger(vol_int, 'D')
        
        self.current_volume -= volume_ul
        
        # 3. 静置
        if self.active_params.settle_delay > 0:
            time.sleep(self.active_params.settle_delay)

        # 4. 吹出 (Blow-out)
        if blow_out:
            if self.active_params.blow_out_volume > 0:
                logger.info("Blowing out...")
                self.driver.move_plunger(int(self.active_params.blow_out_volume), 'D')
                self.current_volume = 0

        if self.current_volume < 0: self.current_volume = 0
        logger.info(f"Dispensed {volume_ul}uL. Remaining: {self.current_volume}")
        return True

    def mix(self, volume: float, cycles: int = 3):
        """混匀"""
        logger.info(f"Mixing {volume}uL x {cycles}...")
        # 混匀使用较快速度
        self.driver.set_max_speed(self.active_params.dispense_speed)
        
        for _ in range(cycles):
            self.driver.move_plunger(int(volume), 'P')
            time.sleep(0.1)
            self.driver.move_plunger(int(volume), 'D')
            time.sleep(0.1)
        
        # 混匀后吹出
        if self.active_params.blow_out_volume > 0:
            self.driver.move_plunger(int(self.active_params.blow_out_volume), 'D')
            self.current_volume = 0