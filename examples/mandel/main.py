
import argparse
import logging
import pathlib
import array
from enum import Enum

from typing import Optional, override

from parasnake.ps_config import PSConfiguration
from parasnake.ps_node import PSNode
from parasnake.ps_server import PSServer
from parasnake.ps_nodeid import PSNodeId


logger = logging.getLogger(__name__)


class MandelInfo:
    def __init__(self, c_start: complex = -2.0-1.5j, c_end: complex = 1.0+1.5j,
            width: int = 1024, height: int = 1024, max_iteration: int = 2048):
        self.c_start: complex = c_start
        self.c_end: complex = c_end
        self.width: int = width
        self.height: int = height
        self.re_step: float = (c_end.real - c_start.real) / float(width)
        self.im_step: float = (c_end.imag - c_start.imag) / float(height)
        self.max_iteration: int = max_iteration


class RowStatus(Enum):
    Empty = 0
    Pending = 1
    Done = 2


class MandelNode(PSNode):
    def __init__(self, config: PSConfiguration):
        super().__init__(config)

    @override
    def ps_init(self, data: MandelInfo):
        self.mandel_info: MandelInfo = data

    @override
    def ps_process_data(self, data: int) -> array.array:
        width: int = self.mandel_info.width
        elems = (0 for _ in range(width))
        line = array.array("L", elems)

        step_x: float = self.mandel_info.re_step
        step_y: float = self.mandel_info.im_step
        c_start: complex = self.mandel_info.c_start
        c_y: float = c_start.imag + (step_y * data)

        for x in range(width):
            c_x: float = c_start.real + (step_x * x)
            c = complex(c_x, c_y)
            z = c
            iter = 0

            while (abs(z) < 2.0) and (iter < self.mandel_info.max_iteration):
                z = c + (z * z)
                iter += 1

            line[x] = iter

        return line


class MandelServer(PSServer):
    def __init__(self, config: PSConfiguration, mandel_info: MandelInfo):
        super().__init__(config)
        self.mandel_info: MandelInfo = mandel_info
        self.node_id_row = {}

        size = mandel_info.width * mandel_info.height
        elems = (0 for _ in range(size))
        self.mandel_image = array.array("L", elems)

        self.processed_rows = [RowStatus.Empty for _ in range(mandel_info.height)]

    @override
    def ps_get_init_data(self, node_id: PSNodeId) -> MandelInfo:
        return self.mandel_info

    @override
    def ps_is_job_done(self) -> bool:
        for status in self.processed_rows:
            if not RowStatus.Done:
                return False

        return True

    @override
    def ps_save_data(self) -> None:
        width: int = self.mandel_info.width
        height: int = self.mandel_info.height

        limit = self.mandel_info.max_iteration
        half_limit: int = int(limit / 2)

        with open("mandel_image.ppm", "w") as f:
            f.write("P3\n")
            f.write(f"{width} {height}\n")
            f.write("255\n")

            for y in range(height):
                for x in range(width):
                    val: int = self.mandel_image[(y * width) + x]

                    if val < self.mandel_info.max_iteration:
                        if val <= half_limit:
                            color_value = int((val * 255) / half_limit)
                            f.write(f"255 0 {color_value} ")
                        else:
                            color_value = int(((limit - val) * 255) / half_limit)
                            f.write(f"{color_value} 0 255 ")
                    else:
                        f.write("0 0 0 ")
                f.write("\n")

    @override
    def ps_node_timeout(self, node_id: PSNodeId) -> None:
        if node_id in self.node_id_row:
            row = self.node_id_row[node_id]
            self.processed_rows[row] = RowStatus.Empty
            del self.node_id_row[node_id]

    @override
    def ps_get_new_data(self, node_id: PSNodeId) -> Optional[int]:
        for i, status in enumerate(self.processed_rows):
            if status == RowStatus.Empty:
                self.node_id_row[node_id] = i
                self.processed_rows[i] = RowStatus.Pending
                return i

        return None
 
    @override
    def ps_process_result(self, node_id: PSNodeId, result: array.array):
        row: int = self.node_id_row[node_id]
        offset: int = (row * self.mandel_info.width)

        for x in range(self.mandel_info.width):
            self.mandel_image[offset + x] = result[x]

        del self.node_id_row[node_id]
        self.processed_rows[row] = RowStatus.Done


def run_server(config: PSConfiguration):
    mandel_info = MandelInfo()
    server = MandelServer(config, mandel_info)
    server.ps_run()

def run_client(config: PSConfiguration):
    node = MandelNode(config)
    node.ps_run()

def main():
    config = PSConfiguration.from_json("mandel_config.json")

    parser = argparse.ArgumentParser(description="Mandelbrot example for parasnake.")
    parser.add_argument("--server", action="store_true", help="Run in server mode. Otherwise client mode (default)")
    args = parser.parse_args()

    log_file_name: str = "mandel_server.log"

    if not args.server:
        cl_num = 1

        while True:
            log_file_name = f"mandel_client_{cl_num}.log"
            p = pathlib.Path(log_file_name)

            if p.is_file():
                cl_num += 1
            else:
                break

    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG, format=log_format)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    if args.server:
        run_server(config)
    else:
        run_client(config)

if __name__ == "__main__":
    main()

