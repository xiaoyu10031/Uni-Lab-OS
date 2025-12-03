#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SynthonX3 GUI (External Front-End)
---------------------------------
为 SynthonX3 提供可视化界面。

架构适配：
- 引用 synthonx3.drivers 和 synthonx3.controllers
- 修复初始化时 load_points 导致的 AttributeError
- 优先使用后端 Controller 提供的运动控制和状态查询接口
"""
from __future__ import annotations
import os
import sys
import time
import json
import math
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from dataclasses import dataclass
from typing import Optional, Dict

# =============================
#  导入 SynthonX3 模块
# =============================
# 为了确保在不同目录下运行都能找到模块，尝试添加当前目录到 sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    # 尝试从当前包结构导入
    from drivers.bus_rs485 import SharedRS485Bus
    from drivers.pipette import SOPAPipetteYYQ
    from drivers.stir import RelayController  # 尝试导入搅拌器驱动
    from controllers.xyz_controller import SharedXYZController, MachineConfig, MotorAxis
except ImportError as e:
    raise RuntimeError(f"无法导入 SynthonX3 模块: {e}\n请确保相关驱动文件(drivers/, controllers/)在 {current_dir} 下") from e


# =============================
#  后端封装（ViewModel）：Station
# =============================
class Station:
    """
    GUI 专用的后端包装类。
    负责管理 SharedRS485Bus、SharedXYZController 和 SOPAPipetteYYQ 的生命周期，
    并为 GUI 提供统一的调用接口。
    """
    def __init__(self, port: str = "/dev/ttyUSB1", baudrate: int = 115200):
        self.port = port
        self.baudrate = baudrate
        self.bus: Optional[SharedRS485Bus] = None
        self.xyz: Optional[SharedXYZController] = None
        self.pip: Optional[SOPAPipetteYYQ] = None
        self.stir: Optional[RelayController] = None # 搅拌器可选
        self.cfg = MachineConfig()
        self.connected = False

    # ---- 连接/断开 ----
    def connect(self) -> bool:
        try:
            self.bus = SharedRS485Bus(self.port, self.baudrate)
            # 打开串口
            if not self.bus.open():
                return False
            
            # 初始化控制器
            self.xyz = SharedXYZController(self.bus, self.cfg)
            self.pip = SOPAPipetteYYQ(self.bus)
            
            # 尝试初始化搅拌器 (假设COM口不同，或者暂时不启用，这里仅作预留)
            # 如果搅拌器需要独立串口，需要在 GUI 上增加配置。这里暂不自动连接搅拌器以防冲突。
            
            self.connected = True
            return True
        except Exception as e:
            print(f"Connect Error: {e}")
            self.connected = False
            return False

    def disconnect(self) -> None:
        if self.bus:
            try:
                self.bus.close()
            except Exception:
                pass
        self.connected = False

    # ---- XYZ 基础 ----
    def set_work_origin_here(self) -> bool:
        if not self.xyz: return False
        return self.xyz.set_work_origin_here()

    def home_safe(self) -> bool:
        """全轴回零（Z→X→Y）。"""
        if not self.xyz: return False
        try:
            return self.xyz.home_all()
        except Exception:
            return False

    def emergency_stop(self) -> bool:
        if not self.xyz: return False
        ok = True
        for ax in (MotorAxis.X, MotorAxis.Y, MotorAxis.Z):
            try:
                self.xyz.emergency_stop(ax)
            except Exception:
                ok = False
        return ok

    # ---- 读取“工作坐标系”下的当前位置(mm) ----
    def get_status_mm(self) -> Dict[str, float]:
        """返回【工作坐标系】下的当前位置 (mm)。"""
        if not self.xyz: return {"x": 0.0, "y": 0.0, "z": 0.0}
        
        try:
            # 优先使用后端提供的计算方法
            pos = self.xyz.get_work_position_mm()
            return {
                "x": float(pos.get("x", 0.0)),
                "y": float(pos.get("y", 0.0)),
                "z": float(pos.get("z", 0.0)),
            }
        except Exception:
            return {"x": 0.0, "y": 0.0, "z": 0.0}

    def get_motor_steps(self) -> Dict[str, int]:
        """获取电机绝对步数（调试用）"""
        if not self.xyz: return {"x": 0, "y": 0, "z": 0}
        try:
            return {
                "x": self.xyz.get_motor_status(MotorAxis.X).steps,
                "y": self.xyz.get_motor_status(MotorAxis.Y).steps,
                "z": self.xyz.get_motor_status(MotorAxis.Z).steps
            }
        except Exception:
            return {"x": 0, "y": 0, "z": 0}

    # ---- 绝对安全移动（调用后端已有策略：抬Z→XY→落Z）----
    def move_to_work_safe(self, x=None, y=None, z=None, speed: Optional[int]=None, acc: Optional[int]=None) -> bool:
        if not self.xyz: return False
        # 注意：后端参数名为 accel
        return self.xyz.move_to_work_safe(x, y, z, speed=speed, accel=acc)

    def move_to_work_direct(self, x=None, y=None, z=None,
                            speed: Optional[int] = None,
                            acc: Optional[int] = None,
                            z_order: str = "auto") -> bool:
        """
        绝对直达：不抬Z。
        """
        if not self.xyz: return False
        return self.xyz.move_to_work_direct(
            x=x, y=y, z=z, speed=speed, accel=acc, z_order=z_order
        )

    # ---- 相对直接移动（快速移动：不抬Z）----
    def move_relative_direct(self, dx: float, dy: float, dz: float, speed: Optional[int]=None, acc: Optional[int]=None) -> bool:
        """基于当前位置直接到新目标（工作坐标 Δmm），不抬Z。"""
        if not self.xyz: return False
        # 调用后端封装好的相对移动逻辑
        return self.xyz.move_relative_direct(dx, dy, dz, speed=speed, accel=acc)

    # ---- Pipette ----
    def pip_init(self) -> bool:
        if not self.pip: return False
        return self.pip.initialize()

    def pip_eject(self) -> bool:
        if not self.pip: return False
        return self.pip.eject_tip()

    def pip_asp(self, ul: float) -> bool:
        if not self.pip: return False
        # 后端 aspirate 没有返回值，通过 try-catch 捕获异常或假设成功
        try:
            self.pip.aspirate(ul)
            return True
        except Exception:
            return False

    def pip_dsp(self, ul: float) -> bool:
        if not self.pip: return False
        try:
            self.pip.dispense(ul)
            return True
        except Exception:
            return False


# =============================
#  GUI Components
# =============================

class LogConsole(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.text = tk.Text(self, height=10)
        self.text.pack(fill=tk.BOTH, expand=True)
    def log(self, s: str):
        ts = time.strftime('%H:%M:%S')
        self.text.insert(tk.END, f"[{ts}] {s}\n")
        self.text.see(tk.END)

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SynthonX3 GUI — 工作坐标控制")
        self.geometry("980x850")
        self.resizable(True, True)

        # 顶部：连接区
        top = ttk.Frame(self)
        top.pack(fill=tk.X, padx=8, pady=6)
        ttk.Label(top, text="串口:").pack(side=tk.LEFT)
        
        default_port = "COM3" if sys.platform == "win32" else "/dev/ttyUSB1"
        self.port_var = tk.StringVar(value=default_port)
        ttk.Entry(top, textvariable=self.port_var, width=14).pack(side=tk.LEFT, padx=6)
        
        ttk.Label(top, text="波特率:").pack(side=tk.LEFT)
        self.baud_var = tk.IntVar(value=115200)
        ttk.Entry(top, textvariable=self.baud_var, width=8).pack(side=tk.LEFT, padx=6)
        
        self.btn_conn = ttk.Button(top, text="连接", command=self.on_connect)
        self.btn_conn.pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="断开", command=self.on_disconnect).pack(side=tk.LEFT, padx=6)
        
        self.lbl_conn = ttk.Label(top, text="未连接", foreground="#B00")
        self.lbl_conn.pack(side=tk.LEFT, padx=10)

        # Notebook
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill=tk.BOTH, expand=True)
        self.tab_xyz = ttk.Frame(self.nb)
        self.tab_pip = ttk.Frame(self.nb)
        self.tab_flow = ttk.Frame(self.nb)
        self.tab_map = ttk.Frame(self.nb)
        self.nb.add(self.tab_xyz, text="XYZ 控制（工作坐标）")
        self.nb.add(self.tab_pip, text="移液枪")
        self.nb.add(self.tab_flow, text="操作向导")
        self.nb.add(self.tab_map, text="示意图（工作坐标）")

        # 日志
        self.console = LogConsole(self)
        self.console.pack(fill=tk.BOTH, expand=False, padx=8, pady=6)

        # Station & 点位
        self.station: Optional[Station] = None
        
        # 优先使用 points.json，如果不存在则回退到 points_gui.json
        base_dir = os.path.dirname(__file__)
        self.points_path = os.path.join(base_dir, "points.json")
        if not os.path.exists(self.points_path):
             self.points_path = os.path.join(base_dir, "points_gui.json")

        self.points: Dict[str, Dict[str, float]] = {}
        
        # 加载点位 (修复处：不依赖 self.station，直接加载)
        self._load_points()

        # 仅在点击后显示的“所选点名”
        self._selected_point_name: str = ""

        # 构建各页
        self._build_xyz_tab()
        self._build_pip_tab()
        self._build_flow_tab()
        self._build_map_tab()

    # ---------- 连接 ----------
    def on_connect(self):
        try:
            self.station = Station(self.port_var.get(), int(self.baud_var.get()))
            if self.station.connect():
                self.lbl_conn.config(text="已连接", foreground="#0A0")
                self.console.log("连接成功（真实串口）")
                self.xyz_refresh() # 连接后尝试读取一次位置
            else:
                self.lbl_conn.config(text="连接失败", foreground="#B00")
                self.console.log("连接失败：请检查端口/波特率/接线")
        except Exception as e:
            self.lbl_conn.config(text="异常", foreground="#B00")
            self.console.log(f"连接异常：{e}")

    def on_disconnect(self):
        if self.station:
            self.station.disconnect()
        self.lbl_conn.config(text="未连接", foreground="#B00")
        self.console.log("已断开")

    # ---------- XYZ Tab ----------
    def _build_xyz_tab(self):
        f = self.tab_xyz
        # 基本
        base = ttk.LabelFrame(f, text="基本")
        base.pack(fill=tk.X, padx=8, pady=8)
        ttk.Button(base, text="设置当前位置为工作原点", command=self.xyz_set_origin).pack(side=tk.LEFT, padx=6, pady=6)
        ttk.Button(base, text="全轴回零 (Z→X→Y)", command=self.xyz_home).pack(side=tk.LEFT, padx=6, pady=6)
        ttk.Button(base, text="紧急停止", command=self.xyz_emg).pack(side=tk.LEFT, padx=6, pady=6)

        # 速度/加速度
        sp = ttk.LabelFrame(f, text="速度/加速度 (rpm / 无量纲)")
        sp.pack(fill=tk.X, padx=8, pady=8)
        self.speed_var = tk.IntVar(value=500)
        self.acc_var = tk.IntVar(value=1000)
        row = ttk.Frame(sp); row.pack(fill=tk.X, padx=4, pady=4)
        ttk.Label(row, text="速度:").pack(side=tk.LEFT)
        ttk.Entry(row, textvariable=self.speed_var, width=8).pack(side=tk.LEFT, padx=6)
        ttk.Label(row, text="加速度:").pack(side=tk.LEFT)
        ttk.Entry(row, textvariable=self.acc_var, width=8).pack(side=tk.LEFT, padx=6)
        
        # 运动区：左=绝对移动，右=相对位移
        mv = ttk.LabelFrame(f, text="移动（工作坐标，mm）：左=绝对 | 右=相对(Δ)")
        mv.pack(fill=tk.X, padx=8, pady=8)

        mv_left = ttk.Frame(mv);  mv_left.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 3), pady=4)
        mv_right = ttk.Frame(mv); mv_right.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(3, 6), pady=4)

        # --- 左侧：绝对移动 ---
        ttk.Label(mv_left, text="绝对目标 (X/Y/Z，mm，工作坐标)").pack(anchor="w")
        self.x_var = tk.DoubleVar(value=0.0)
        self.y_var = tk.DoubleVar(value=0.0)
        self.z_var = tk.DoubleVar(value=0.0)
        for label, var in (("X", self.x_var),("Y", self.y_var),("Z", self.z_var)):
            row = ttk.Frame(mv_left); row.pack(fill=tk.X, padx=0, pady=3)
            ttk.Label(row, text=f"{label}=").pack(side=tk.LEFT)
            ttk.Entry(row, textvariable=var, width=10).pack(side=tk.LEFT)

        # 勾选后不抬Z：绝对“直达”
        self.direct_abs_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            mv_left,
            text="不抬Z轴（绝对直达，按Z高低决定先后）",
            variable=self.direct_abs_var
        ).pack(anchor="w", pady=(4,2))

        ttk.Button(mv_left, text="执行绝对移动", command=self.xyz_move_absolute).pack(anchor="w", pady=(2,0))

        # --- 右侧：相对位移 ---
        ttk.Label(mv_right, text="相对位移 Δ (mm，工作坐标)").pack(anchor="w")
        self.rx_var = tk.DoubleVar(value=0.0)
        self.ry_var = tk.DoubleVar(value=0.0)
        self.rz_var = tk.DoubleVar(value=0.0)
        for label, var in (("ΔX", self.rx_var),("ΔY", self.ry_var),("ΔZ", self.rz_var)):
            row = ttk.Frame(mv_right); row.pack(fill=tk.X, padx=0, pady=3)
            ttk.Label(row, text=f"{label}=").pack(side=tk.LEFT)
            ttk.Entry(row, textvariable=var, width=10).pack(side=tk.LEFT)

        ttk.Button(mv_right, text="执行相对位移", command=self.xyz_move_relative_inputs).pack(anchor="w", pady=(2,0))

        # 点位管理
        pm = ttk.LabelFrame(f, text="位置点管理（JSON，工作坐标）")
        pm.pack(fill=tk.X, padx=8, pady=8)
        self.point_name_var = tk.StringVar(value="")
        row1 = ttk.Frame(pm); row1.pack(fill=tk.X, padx=4, pady=3)
        ttk.Label(row1, text="点名:").pack(side=tk.LEFT)
        ttk.Entry(row1, textvariable=self.point_name_var, width=18).pack(side=tk.LEFT, padx=6)
        ttk.Button(row1, text="保存当前 X/Y/Z 为该点", command=self.save_point).pack(side=tk.LEFT, padx=6)
        row2 = ttk.Frame(pm); row2.pack(fill=tk.X, padx=4, pady=3)
        ttk.Label(row2, text="点列表:").pack(side=tk.LEFT)
        self.point_combo = ttk.Combobox(row2, values=sorted(self.points.keys()), width=20)
        self.point_combo.pack(side=tk.LEFT, padx=6)
        ttk.Button(row2, text="移动到点(安全)", command=self.move_to_point).pack(side=tk.LEFT, padx=6)
        ttk.Button(row2, text="删除点", command=self.delete_point).pack(side=tk.LEFT, padx=6)
        ttk.Button(row2, text="刷新列表", command=self.refresh_points_combo).pack(side=tk.LEFT, padx=6)

        # 状态读取
        st = ttk.LabelFrame(f, text="当前位置（工作坐标 mm / 步）")
        st.pack(fill=tk.X, padx=8, pady=8)
        self.lbl_pos = ttk.Label(st, text="X: -, Y: -, Z: -")
        self.lbl_pos.pack(side=tk.LEFT, padx=6)
        ttk.Button(st, text="刷新", command=self.xyz_refresh).pack(side=tk.LEFT, padx=6)

    # XYZ 事件
    def xyz_move_absolute(self):
        s = self._need_station();  
        if not s: return
        try:
            sp = int(self.speed_var.get())
            ac = int(self.acc_var.get())
            x, y, z = self.x_var.get(), self.y_var.get(), self.z_var.get()
            
            # 检查是否直达
            if getattr(self, "direct_abs_var", None) and self.direct_abs_var.get():
                ok = s.move_to_work_direct(x, y, z, speed=sp, acc=ac)
                method_name = "绝对直达（不抬Z）"
            else:
                ok = s.move_to_work_safe(x, y, z, speed=sp, acc=ac)
                method_name = "安全移动（抬Z）"
            
            self.console.log(f"{method_name}：{'OK' if ok else 'Fail'} (x={x}, y={y}, z={z}, speed={sp}, acc={ac})")
            self.xyz_refresh()
        except Exception as e:
            messagebox.showerror("移动失败", str(e))
            self.console.log(f"移动失败：{e}")

    def xyz_move_relative_inputs(self):
        s = self._need_station();  
        if not s: return
        try:
            sp = int(self.speed_var.get())
            ac = int(self.acc_var.get())
            dx, dy, dz = self.rx_var.get(), self.ry_var.get(), self.rz_var.get()
            ok = s.move_relative_direct(dx, dy, dz, speed=sp, acc=ac)
            if ok:
                pos = s.get_status_mm()
                self.console.log(
                    f"相对位移：OK (Δx={dx}, Δy={dy}, Δz={dz}) → 绝对(x={pos['x']:.2f}, y={pos['y']:.2f}, z={pos['z']:.2f})"
                )
                self.xyz_refresh()
            else:
                self.console.log(f"相对位移：Fail (Δx={dx}, Δy={dy}, Δz={dz})")
        except Exception as e:
            messagebox.showerror("相对位移失败", str(e))
            self.console.log(f"相对位移失败：{e}")

    def xyz_set_origin(self):
        s = self._need_station();  
        if not s: return
        s.set_work_origin_here()
        self.console.log("工作原点已更新为当前位置")
        self.xyz_refresh()

    def xyz_home(self):
        s = self._need_station();  
        if not s: return
        self.console.log("开始回零...")
        # 运行中不阻塞GUI需要线程，这里为简单直接调用
        if s.home_safe():
            self.console.log("全轴回零 (Z→X→Y) 完成")
            self.xyz_refresh()
        else:
            self.console.log("全轴回零失败")

    def xyz_emg(self):
        s = self._need_station();  
        if not s: return
        ok = s.emergency_stop()
        self.console.log(f"紧急停止：{'OK' if ok else 'Fail'}")

    def xyz_refresh(self):
        s = self._need_station();  
        if not s: return
        try:
            pos = s.get_status_mm()  # 工作坐标
            steps = s.get_motor_steps() # 绝对步数
            
            self.lbl_pos.config(text=(
                f"工作坐标  X:{pos['x']:.2f} mm ({steps['x']}步)  "
                f"Y:{pos['y']:.2f} mm ({steps['y']}步)  "
                f"Z:{pos['z']:.2f} mm ({steps['z']}步)"
            ))
        except Exception as e:
            # messagebox.showerror("读取失败", str(e))
            self.console.log(f"读取状态失败: {e}")

    # ---------- Pipette Tab ----------
    def _build_pip_tab(self):
        f = self.tab_pip
        base = ttk.LabelFrame(f, text="基础")
        base.pack(fill=tk.X, padx=8, pady=8)
        ttk.Button(base, text="初始化", command=self.pip_init).pack(side=tk.LEFT, padx=6, pady=6)
        ttk.Button(base, text="弹出枪头", command=self.pip_eject).pack(side=tk.LEFT, padx=6, pady=6)
        
        ops = ttk.LabelFrame(f, text="吸/排液 (uL)")
        ops.pack(fill=tk.X, padx=8, pady=8)
        self.asp_var = tk.DoubleVar(value=100)
        self.dsp_var = tk.DoubleVar(value=100)
        ttk.Label(ops, text="吸液:").pack(side=tk.LEFT)
        ttk.Entry(ops, textvariable=self.asp_var, width=8).pack(side=tk.LEFT)
        ttk.Button(ops, text="执行吸液", command=self.pip_asp).pack(side=tk.LEFT, padx=6)
        ttk.Label(ops, text="排液:").pack(side=tk.LEFT)
        ttk.Entry(ops, textvariable=self.dsp_var, width=8).pack(side=tk.LEFT)
        ttk.Button(ops, text="执行排液", command=self.pip_dsp).pack(side=tk.LEFT, padx=6)
        
        st = ttk.LabelFrame(f, text="状态")
        st.pack(fill=tk.X, padx=8, pady=8)
        self.lbl_pip = ttk.Label(st, text="-")
        self.lbl_pip.pack(side=tk.LEFT, padx=6, pady=6)

    def pip_init(self):
        s = self._need_station();  
        if not s: return
        if s.pip_init():
            self.console.log("移液枪初始化完成")
            self.lbl_pip.config(text="已初始化")
        else:
            self.console.log("移液枪初始化失败")

    def pip_eject(self):
        s = self._need_station();  
        if not s: return
        if s.pip_eject():
            self.console.log("枪头弹出指令已发送")
        else:
            self.console.log("枪头弹出失败")

    def pip_asp(self):
        s = self._need_station();  
        if not s: return
        v = self.asp_var.get()
        if s.pip_asp(v):
            self.console.log(f"吸液 {v} uL 完成")
        else:
            self.console.log("吸液失败")

    def pip_dsp(self):
        s = self._need_station();  
        if not s: return
        v = self.dsp_var.get()
        if s.pip_dsp(v):
            self.console.log(f"排液 {v} uL 完成")
        else:
            self.console.log("排液失败")

    # ---------- Flow Tab ----------
    def _build_flow_tab(self):
        f = self.tab_flow
        box = ttk.LabelFrame(f, text="Transfer Demo")
        box.pack(fill=tk.X, padx=8, pady=8)
        ttk.Label(box, text="演示：吸 100 → 排 100（需已装枪头）").pack(side=tk.LEFT, padx=6)
        ttk.Button(box, text="Run", command=self.flow_demo).pack(side=tk.LEFT, padx=6)

    def flow_demo(self):
        s = self._need_station();  
        if not s: return
        s.pip_asp(100)
        time.sleep(0.5)
        s.pip_dsp(100)
        self.console.log("Transfer demo 完成")

    # ---------- Map Tab ----------
    def _build_map_tab(self):
        f = self.tab_map
        container = ttk.Frame(f); container.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        left = ttk.Frame(container); right = ttk.Frame(container)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        right.pack(side=tk.LEFT, fill=tk.Y, padx=10)
        self.map_canvas = tk.Canvas(left, width=640, height=480, background="white", highlightthickness=1, highlightbackground="#999")
        self.map_canvas.pack(fill=tk.BOTH, expand=True)
        self.map_canvas.bind("<Button-1>", self.map_on_click)
        self.map_canvas.bind("<Configure>", lambda e: self.draw_map())
        
        ttk.Label(right, text="所选点：").pack(anchor="w", pady=(4,0))
        self.map_selected = tk.StringVar(value="")
        ttk.Label(right, textvariable=self.map_selected).pack(anchor="w")
        
        ttk.Button(right, text="刷新示意图", command=self.draw_map).pack(fill=tk.X, pady=6)
        ttk.Separator(right, orient="horizontal").pack(fill=tk.X, pady=8)
        ttk.Button(right, text="安全运动到所选点（绝对）", command=self.map_move_safe).pack(fill=tk.X, pady=6)
        ttk.Button(right, text="相对运动到所选点（Δ=目标-当前）", command=self.map_move_relative).pack(fill=tk.X, pady=6)
        
        tip = "示意图仅展示 XY 平面；点击点后会在画布上显示该点名称。\n安全移动=抬Z-XY-落Z；相对移动=直接Δmm。\n所有坐标均为【工作坐标】。"
        ttk.Label(right, text=tip, wraplength=240, foreground="#555").pack(fill=tk.X, pady=8)
        self.draw_map()

    def _workspace_xy(self):
        try:
            if self.station and self.station.xyz:
                mc = self.station.cfg
            else:
                mc = MachineConfig()
            return float(mc.max_travel_x), float(mc.max_travel_y)
        except Exception:
            return 340.0, 250.0

    def _xy_to_canvas(self, x_mm, y_mm, cw, ch, scale, margin):
        cx = margin + x_mm * scale
        cy = ch - margin - y_mm * scale  # 画布 y 向下
        return cx, cy

    def draw_map(self):
        cnv = getattr(self, 'map_canvas', None)
        if not cnv: return
        cnv.delete("all")
        cw = cnv.winfo_width() or 640
        ch = cnv.winfo_height() or 480
        margin = 40
        max_x, max_y = self._workspace_xy()
        if max_x <= 0 or max_y <= 0: max_x, max_y = 340.0, 250.0
        
        scale = min((cw-2*margin)/max_x, (ch-2*margin)/max_y)
        
        # 边框
        cnv.create_rectangle(margin, margin, cw-margin, ch-margin, outline="#333", width=2)
        # 外侧标注为“工作坐标系”
        cnv.create_text(cw - margin, ch - 6, anchor="se", text="工作坐标系 (mm)", fill="#333")
        
        # 点（默认只画点，不显示文字；仅被选中的点显示名称）
        self._map_item_to_name = {}
        r = 5
        for name, p in sorted(self.points.items()):
            try:
                x, y = float(p.get("x", 0.0)), float(p.get("y", 0.0))
            except Exception:
                continue
            cx, cy = self._xy_to_canvas(x, y, cw, ch, scale, margin)
            
            # 选中点：红色加粗并显示名称；未选中：蓝色
            sel = (name == self._selected_point_name)
            color = "#ff375f" if sel else "#0a84ff"
            width = 3 if sel else 2
            item = cnv.create_oval(cx-r, cy-r, cx+r, cy+r, outline=color, width=width, tags=("point",))
            self._map_item_to_name[item] = name
            if sel:
                cnv.create_text(cx+10, cy-12, anchor="w", text=name, fill="#333")  # 仅选中时显示名称

        # 当前坐标十字（工作坐标）
        try:
            if self.station and self.station.connected:
                cur = self.station.get_status_mm()  # 工作坐标
                cx, cy = self._xy_to_canvas(cur['x'], cur['y'], cw, ch, scale, margin)
                cnv.create_line(cx-8, cy, cx+8, cy, width=2)
                cnv.create_line(cx, cy-8, cx, cy+8, width=2)
                cnv.create_text(cx+12, cy-10, anchor="w", text=f"当前({cur['x']:.1f},{cur['y']:.1f})")
        except Exception:
            pass

    def map_on_click(self, event):
        cnv = getattr(self, 'map_canvas', None)
        if not cnv: return
        item = cnv.find_closest(event.x, event.y)
        if not item: return
        iid = item[0]
        if "point" not in cnv.gettags(iid):
            return
        # 通过映射拿到点名
        name = self._map_item_to_name.get(iid, "")
        # 记录并刷新（仅选中时显示名称）
        self._selected_point_name = name
        self.map_selected.set(name)
        self.draw_map()

    def map_move_safe(self):
        s = self._need_station();  
        if not s: return
        name = self.map_selected.get().strip()
        if name not in self.points:
            messagebox.showwarning("未选择点", "请先在示意图上点击一个点"); return
        p = self.points[name]
        sp, ac = int(self.speed_var.get()), int(self.acc_var.get())
        try:
            ok = s.move_to_work_safe(p['x'], p['y'], p['z'], speed=sp, acc=ac)
            self.console.log(f"[示意图] 安全移动到 '{name}'（工作坐标）: {'OK' if ok else 'Fail'} → (x={p['x']}, y={p['y']}, z={p['z']})")
            self.xyz_refresh()
        except Exception as e:
            messagebox.showerror("移动失败", str(e))
            self.console.log(f"[示意图] 移动失败：{e}")

    def map_move_relative(self):
        s = self._need_station();  
        if not s: return
        name = self.map_selected.get().strip()
        if name not in self.points:
            messagebox.showwarning("未选择点", "请先在示意图上点击一个点"); return
        p = self.points[name]
        try:
            cur = s.get_status_mm()  # 工作坐标
            dx, dy, dz = p['x']-cur['x'], p['y']-cur['y'], p['z']-cur['z']
        except Exception:
            dx, dy, dz = p['x'], p['y'], p['z']
        sp, ac = int(self.speed_var.get()), int(self.acc_var.get())
        try:
            ok = s.move_relative_direct(dx, dy, dz, speed=sp, acc=ac)
            if ok:
                pos = s.get_status_mm()
                self.console.log(
                    f"[示意图] 相对移动至 '{name}'（工作坐标）: OK (Δx={dx:.2f}, Δy={dy:.2f}, Δz={dz:.2f}) → 绝对(x={pos['x']:.2f}, y={pos['y']:.2f}, z={pos['z']:.2f})"
                )
                self.xyz_refresh()
                try: self.draw_map()
                except Exception: pass
            else:
                self.console.log(f"[示意图] 相对移动至 '{name}'（工作坐标）: Fail (Δx={dx:.2f}, Δy={dy:.2f}, Δz={dz:.2f})")
        except Exception as e:
            messagebox.showerror("相对移动失败", str(e))
            self.console.log(f"[示意图] 相对移动失败：{e}")

    # ---------- 点位存取 ----------
    # 修复：直接在 App 中读写文件，不依赖 Station 实例
    def _load_points(self):
        if os.path.exists(self.points_path):
            try:
                with open(self.points_path, "r", encoding="utf-8") as f:
                    self.points = json.load(f)
                # self.console.log(f"已加载点位: {self.points_path}")
            except Exception as e:
                self.points = {}
                self.console.log(f"加载点位失败: {e}")
        else:
            self.points = {}
        
        # 若组合框已创建，刷新
        if hasattr(self, 'point_combo'):
            self.refresh_points_combo()

    def _save_points(self):
        try:
            with open(self.points_path, "w", encoding="utf-8") as f:
                json.dump(self.points, f, ensure_ascii=False, indent=2)
            self.console.log(f"点位已保存到 {self.points_path}")
        except Exception as e:
            messagebox.showerror("保存失败", str(e))
            self.console.log(f"点位保存失败：{e}")

    def save_point(self):
        s = self._need_station();  
        if not s: return
        name = (self.point_name_var.get() or '').strip()
        if not name:
            messagebox.showwarning("无名称", "请先输入点名称"); return
        pos = s.get_status_mm()  # 工作坐标
        self.points[name] = {"x": pos['x'], "y": pos['y'], "z": pos['z']}
        self._save_points(); self.refresh_points_combo()
        self.console.log(f"保存点 '{name}'（工作坐标）: x={pos['x']:.3f}, y={pos['y']:.3f}, z={pos['z']:.3f}")

    def move_to_point(self):
        s = self._need_station();  
        if not s: return
        name = (self.point_combo.get() or self.point_name_var.get()).strip()
        if name not in self.points:
            messagebox.showwarning("未找到点", f"未找到点: {name}"); return
        p = self.points[name]
        sp, ac = int(self.speed_var.get()), int(self.acc_var.get())
        try:
            ok = s.move_to_work_safe(p['x'], p['y'], p['z'], speed=sp, acc=ac)
            self.console.log(f"移动到点 '{name}'（工作坐标）: {'OK' if ok else 'Fail'} → (x={p['x']}, y={p['y']}, z={p['z']})")
            self.xyz_refresh()
        except Exception as e:
            messagebox.showerror("移动失败", str(e))
            self.console.log(f"移动失败：{e}")

    def delete_point(self):
        name = (self.point_combo.get() or self.point_name_var.get()).strip()
        if name and name in self.points:
            del self.points[name]
            self._save_points(); self.refresh_points_combo()
            self.console.log(f"已删除点 '{name}'")
            # 若删除的是已选点，清空选择
            if name == self._selected_point_name:
                self._selected_point_name = ""
                self.map_selected.set("")
                self.draw_map()
        else:
            messagebox.showwarning("未找到点", f"未找到点: {name}")

    def refresh_points_combo(self):
        if hasattr(self, 'point_combo'):
            names = sorted(self.points.keys())
            self.point_combo['values'] = names
            if names and (self.point_combo.get() not in names):
                self.point_combo.set(names[0])
        self.draw_map()

    # ---------- helpers ----------
    def _need_station(self) -> Optional[Station]:
        if not self.station or not self.station.connected:
            messagebox.showwarning("未连接", "请先连接设备")
            return None
        return self.station


def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()