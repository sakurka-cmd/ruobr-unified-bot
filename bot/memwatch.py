#!/usr/bin/env python3
"""
memwatch — диагностика утечек памяти для ruobr-bot.

Что делает:
  1. Включает tracemalloc (25 кадров) с момента старта процесса.
  2. Каждые MEMWATCH_INTERVAL секунд пишет в лог:
     - RSS процесса, tracemalloc current/peak
     - статистику GC (кол-во объектов, счётчики сборок поколений)
     - top-5 мест аллокаций (по текущему размеру)
  3. По сигналу SIGUSR1 пишет полный отчёт в data_dir/memwatch_report.txt:
     - top-30 трассировок по текущему размеру (с traceback)
     - diff top-30 относительно базового снапшота (рост с момента старта)
     - гистограмму типов объектов GC

Запуск: из main.py после setup_logging():
    from bot.memwatch import memwatch
    memwatch.start()
    ...
    memwatch.install_signal_handler()  # SIGUSR1 = полный отчёт

Разрешение проблем:
  - Если RSS превысил MEMWATCH_SOFT_LIMIT_MB — log CRITICAL (before cgroup OOM-kill
    at MemoryMax=700M) и graceful exit(75), systemd перезапустит сервис.
"""
import asyncio
import gc
import logging
import os
import signal
import sys
import time
import tracemalloc
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)

MEMWATCH_INTERVAL = int(os.getenv("MEMWATCH_INTERVAL", "900"))      # 15 мин
MEMWATCH_SOFT_LIMIT_MB = int(os.getenv("MEMWATCH_SOFT_LIMIT_MB", "620"))
TRACEMALLOC_FRAMES = 25

PROCESS_START = time.time()


def _rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except Exception:
        pass
    return 0.0


def _gc_stats() -> str:
    counts = gc.get_count()
    stats = gc.get_stats()
    coll = [s["collections"] for s in stats]
    nobj = len(gc.get_objects())
    return (f"gc_counts={counts} collections={coll} tracked_objects={nobj}")


def _malloc_trim() -> float:
    """Вызывает malloc_trim(0) — возвращает освобождённый объём в MB.
    Если много — память удерживает фрагментация glibc-арен, а не живые объекты."""
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        before = _rss_mb()
        libc.malloc_trim(0)
        after = _rss_mb()
        return before - after
    except Exception as e:
        logger.warning(f"malloc_trim failed: {e}")
        return 0.0


def _top_lines(snapshot, limit=5):
    stats = snapshot.statistics("lineno")
    lines = []
    for st in stats[:limit]:
        frame = st.traceback[0]
        lines.append(f"    {st.size/1024:10.1f} KB  {st.count:9d} objs  {frame.filename}:{frame.lineno}")
    return lines


class MemWatch:
    def __init__(self):
        self._baseline = None
        self._baseline_time = None
        self._task = None
        self._report_path = None

    def start(self, data_dir=None):
        tracemalloc.start(TRACEMALLOC_FRAMES)
        from pathlib import Path
        dd = Path(data_dir) if data_dir else Path("data-prod")
        self._report_path = str(dd / "memwatch_report.txt")
        logger.info(
            f"memwatch: tracemalloc enabled ({TRACEMALLOC_FRAMES} frames), "
            f"interval={MEMWATCH_INTERVAL}s, soft_limit={MEMWATCH_SOFT_LIMIT_MB}MB, "
            f"report={self._report_path}"
        )

        import asyncio
        self._task = asyncio.get_running_loop().create_task(self._loop())


    def install_signal_handler(self, sig=signal.SIGUSR1):
        try:
            loop = asyncio.get_running_loop()
            for s in (sig,):
                try:
                    loop.add_signal_handler(s, self.write_report)
                except NotImplementedError:
                    signal.signal(s, lambda *_: self.write_report())
            logger.info(f"memwatch: report signal handler installed (kill -USR1 {os.getpid()})")
        except Exception as e:
            logger.warning(f"memwatch: signal handler install failed: {e}")

    def install_trim_handler(self, sig=signal.SIGUSR2):
        """SIGUSR2 → malloc_trim(0): диагностика фрагментации (RSS до/после в лог)."""
        def do_trim():
            freed = _malloc_trim()
            logger.info(f"memwatch: malloc_trim freed {freed:.0f}MB, rss now {_rss_mb():.0f}MB")
        try:
            loop = asyncio.get_running_loop()
            try:
                loop.add_signal_handler(sig, do_trim)
            except NotImplementedError:
                signal.signal(sig, lambda *_: do_trim())
            logger.info(f"memwatch: trim handler installed (kill -USR2 {os.getpid()})")
        except Exception as e:
            logger.warning(f"memwatch: trim handler install failed: {e}")

    async def _loop(self):
        n = 0
        while True:
            try:
                await asyncio.sleep(MEMWATCH_INTERVAL)
                n += 1
                rss = _rss_mb()
                cur, peak = tracemalloc.get_traced_memory()
                msg = (
                    f"memwatch #{n}: rss={rss:.0f}MB traced_current={cur/1048576:.0f}MB "
                    f"traced_peak={peak/1048576:.0f}MB uptime={int((time.time()-PROCESS_START)/3600)}h | {_gc_stats()}"
                )
                if rss > MEMWATCH_SOFT_LIMIT_MB:
                    logger.critical(msg + " | SOFT LIMIT EXCEEDED — graceful restart")
                    logger.critical("memwatch: final report:\n" + "\n".join(self._full_report_lines(limit=15)))
                    os._exit(75)  # systemd Restart=always поднимет процесс
                logger.info(msg)
                if n == 4 and self._baseline is None:
                    # Базовый снапшот через ~1 час — стартовые аллокации уже устаканились
                    self._baseline = tracemalloc.take_snapshot()
                    self._baseline_time = time.time()
                    logger.info("memwatch: baseline snapshot taken")
                if n % 4 == 0:
                    logger.info("memwatch top-5 (current):\n" + "\n".join(_top_lines(tracemalloc.take_snapshot())))
                # Раз в час пробуем trim — если освобождает >50MB, это фрагментация
                if n % 4 == 2:
                    freed = _malloc_trim()
                    if freed > 50:
                        logger.info(f"memwatch: periodic malloc_trim freed {freed:.0f}MB — fragmentation suspected")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(f"memwatch loop error: {e}")

    def _full_report_lines(self, limit=30):
        lines = []
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rss = _rss_mb()
        cur, peak = tracemalloc.get_traced_memory()
        lines.append(f"===== memwatch report {ts} | rss={rss:.0f}MB cur={cur/1048576:.0f}MB peak={peak/1048576:.0f}MB =====")
        lines.append(f"gc: {_gc_stats()}")

        snap = tracemalloc.take_snapshot()
        lines.append("--- TOP current allocations (lineno) ---")
        for st in snap.statistics("lineno")[:limit]:
            f0 = st.traceback[0]
            lines.append(f"  {st.size/1048576:8.2f} MB  {st.count:9d}  {f0.filename}:{f0.lineno}")

        if self._baseline is not None:
            hours = (time.time() - self._baseline_time) / 3600
            lines.append(f"--- TOP growth since baseline ({hours:.1f}h ago, lineno) ---")
            diffs = snap.compare_to(self._baseline, "lineno")
            diffs = [d for d in diffs if d.size_diff > 0]
            diffs.sort(key=lambda d: -d.size_diff)
            for d in diffs[:limit]:
                f0 = d.traceback[0]
                lines.append(f"  +{d.size_diff/1048576:8.2f} MB ({d.size/1048576:8.2f} MB now)  {f0.filename}:{f0.lineno}")

            lines.append(f"--- TOP growth since baseline (traceback) ---")
            diffs = snap.compare_to(self._baseline, "traceback")
            diffs = [d for d in diffs if d.size_diff > 0]
            diffs.sort(key=lambda d: -d.size_diff)
            for d in diffs[:limit // 2]:
                lines.append(f"  +{d.size_diff/1048576:8.2f} MB ({d.size/1048576:8.2f} MB now)")
                for fr in d.traceback[-4:]:
                    lines.append(f"      {fr.filename}:{fr.lineno}")

        # Типы объектов
        type_counts = Counter(type(o).__name__ for o in gc.get_objects())
        lines.append("--- GC object types (top 20) ---")
        for tname, cnt in type_counts.most_common(20):
            lines.append(f"  {cnt:10d}  {tname}")
        return lines

    def write_report(self):
        try:
            lines = self._full_report_lines()
            if self._report_path:
                with open(self._report_path, "a") as f:
                    f.write("\n".join(lines) + "\n\n")
            logger.info("memwatch report:\n" + "\n".join(lines))
        except Exception as e:
            logger.error(f"memwatch report failed: {e}")


memwatch = MemWatch()
