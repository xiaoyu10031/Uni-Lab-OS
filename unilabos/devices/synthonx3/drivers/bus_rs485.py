import threading
import time
try:
    import serial
except Exception as e:
    raise RuntimeError("Please install pyserial: pip install pyserial") from e
import logging
logger = logging.getLogger("liquid_station.rs45")

class SharedRS485Bus:
    """
    所有设备（XYZ轴+移液枪使用同一个485接口，加入线程锁保证同时只进行一个请求
    """

    def __init__(self, port: str = "/dev/ttyUSB1", baudrate: int = 115200, timeout: float = 0.2):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial = None
        self.lock = threading.Lock()

    def open(self):
        '''
        开启串口,数据格式为:1 位起始位,1 位停止位,8 位数据位,无奇偶校验。 
        '''
        if self.serial and self.serial.is_open:
            return True
        self.serial = serial.Serial(
            port=self.port, baudrate=self.baudrate,
            bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE, timeout=self.timeout
        )
        logger.info(f"Opened RS485 bus on {self.port}")
        return True

    def close(self):
        if self.serial and self.serial.is_open:
            self.serial.close()
            logger.info("Closed RS485 bus")

    def reset_input(self):
        '''
        清空缓存数据
        '''
        if self.serial:
            self.serial.reset_input_buffer()

    def write(self, data: bytes):
        '''
        发送数据
        '''
        self.serial.write(data)

    def read(self, n: int = 256) -> bytes:
        '''
        读取数据
        '''
        return self.serial.read(n)

    def read_exact(self, n: int, overall_timeout: float = 0.3) -> bytes:
        '''
        精确读取指定字节数，带整体超时。
        参数:
            n: 期望读取的字节数（<=0 则直接返回 b''）
            overall_timeout: 整体超时时间（秒），超时返回已读数据
        返回:
            实际读取到的字节串；若超时且无数据则为空 b''
        '''
        if n <= 0:
            return b""
        buf = b""
        deadline = time.time() + overall_timeout    # 计算截止时间
        while len(buf) < n:
            if time.time() > deadline:    # 超时退出
                break 
            need = n - len(buf)    # 还需读取的字节数
            chunk = self.read(need)  # 读取
            if chunk:
                buf += chunk   # 追加有效数据
            else:
                time.sleep(0.001)   # 无数据时小睡，避免忙等
        return buf