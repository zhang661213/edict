#!/usr/bin/env python3
"""Refresh Watcher — 常驻进程，监控信号文件，debounce 后执行 refresh_live_data.py。

替代 kanban_update.py 中每次操作都 fork 子进程的方式。
多 Agent 并发时，200 次 touch → 合并为 1 次 refresh。

运行方式:
  python3 scripts/refresh_watcher.py

部署方式:
  - systemd: 参见 edict.service
  - docker-compose: 参见 edict/docker-compose.yml
  - 手动前台: python3 scripts/refresh_watcher.py
"""
import logging
import os
import pathlib
import signal
import subprocess
import sys
import time

_BASE = pathlib.Path(os.environ.get('EDICT_HOME', '')).resolve() if os.environ.get('EDICT_HOME') else pathlib.Path(__file__).resolve().parent.parent
SIGNAL_FILE = _BASE / 'data' / '.refresh_pending'
PID_FILE = _BASE / 'data' / '.refresh_watcher_pid'
REFRESH_SCRIPT = _BASE / 'scripts' / 'refresh_live_data.py'
DEBOUNCE_SEC = 2       # 信号文件稳定 2 秒后才执行
POLL_INTERVAL = 0.5    # 检查间隔

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [refresh_watcher] %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger('refresh_watcher')

_running = True


def _shutdown(signum, frame):
    global _running
    _running = False
    log.info(f'收到信号 {signum}，准备退出')


def main():
    # 写 PID 文件，让 kanban_update.py 知道 watcher 在运行
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))
    log.info(f'Refresh watcher started (pid={os.getpid()}, debounce={DEBOUNCE_SEC}s)')

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    last_seen_mtime = 0.0
    refresh_count = 0

    try:
        while _running:
            try:
                if SIGNAL_FILE.exists():
                    mtime = SIGNAL_FILE.stat().st_mtime
                    now = time.time()
                    # 信号文件存在且已稳定 DEBOUNCE_SEC 秒
                    if mtime > last_seen_mtime and (now - mtime) >= DEBOUNCE_SEC:
                        last_seen_mtime = mtime
                        # 删除信号文件（在执行前删，避免执行期间的新 touch 被吞）
                        try:
                            SIGNAL_FILE.unlink()
                        except FileNotFoundError:
                            pass

                        refresh_count += 1
                        log.info(f'🔄 执行 refresh #{refresh_count}')
                        try:
                            subprocess.run(
                                [sys.executable, str(REFRESH_SCRIPT)],
                                capture_output=True,
                                timeout=30,
                            )
                        except subprocess.TimeoutExpired:
                            log.warning('refresh_live_data.py 超时 (30s)')
                        except Exception as e:
                            log.error(f'refresh 执行失败: {e}')
            except Exception as e:
                log.error(f'Watcher loop error: {e}')

            time.sleep(POLL_INTERVAL)
    finally:
        # 清理 PID 文件
        try:
            PID_FILE.unlink()
        except Exception:
            pass
        log.info(f'Refresh watcher stopped (total refreshes: {refresh_count})')


def health_check():
    """健康检查：验证 watcher 进程是否正常运转。
    
    检查项：
    1. PID 文件存在且进程存活
    2. 信号文件可写
    3. 刷新脚本可执行
    
    返回: (ok: bool, message: str)
    """
    # 检查 PID 文件
    if not PID_FILE.exists():
        return False, "PID 文件不存在，watcher 未运行"
    try:
        pid = int(PID_FILE.read_text().strip())
    except (ValueError, IOError):
        return False, "PID 文件读取失败"
    
    # 检查进程存活
    try:
        os.kill(pid, 0)  # signal 0 只检测进程是否存在
    except OSError:
        return False, f"进程 {pid} 已不存在"
    
    # 检查信号文件可写
    try:
        SIGNAL_FILE.touch()
        SIGNAL_FILE.unlink()
    except OSError:
        return False, "信号文件不可写"
    
    # 检查刷新脚本存在
    if not REFRESH_SCRIPT.exists():
        return False, "refresh_live_data.py 不存在"
    
    return True, f"Watcher 运行正常 (pid={pid})"


if __name__ == '__main__':
    if '--health' in sys.argv:
        ok, msg = health_check()
        print(msg)
        sys.exit(0 if ok else 1)
    else:
        main()
