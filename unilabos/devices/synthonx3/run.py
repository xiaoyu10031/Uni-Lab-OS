import logging
import argparse
from services.station import Station
from services.flows import Flow

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="SynthonX Automation CLI")
    parser.add_argument("--port", default="/dev/ttyUSB1", help="Serial port (e.g., /dev/ttyUSB0 or COM3)")
    parser.add_argument("--init", action="store_true", help="Run homing sequence")
    parser.add_argument("--demo", action="store_true", help="Run A1->B1 transfer demo")
    parser.add_argument("--full", action="store_true", help="Run the FULL original sequence")
    args = parser.parse_args()

    # 1. 实例化 Station (这里只负责组装硬件，不含流程逻辑)
    station = Station(port=args.port, points_file="unilabos/devices/synthonx3/points.json")
    
    try:
        station.connect()
        
        # 2. 实例化 Flow (注入 Station)
        flow = Flow(station)
        if args.full:
            flow.run_full_sequence()

        if args.init:
            flow.system_init()

        if args.demo:
            flow.run_demo_sequence()
            
        # 如果没有参数，进入交互模式
        if not (args.init or args.demo):
            print("\n=== 交互模式 ===")
            print("可用变量: station, flow, xyz, pip")
            print("示例: station.move_to_point('A1')")
            print("示例: pip.aspirate(100)")
            # 进入 IPython 或 简单 Input 循环
            import code
            xyz = station.xyz
            pip = station.pip
            code.interact(local=locals())

    except Exception as e:
        logging.error(f"Critical Error: {e}")
    finally:
        station.disconnect()

if __name__ == "__main__":
    main()