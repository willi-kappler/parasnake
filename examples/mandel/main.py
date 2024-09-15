
import argparse
import logging
import pathlib
from typing import Optional, override

from parasnake.ps_config import PSConfiguration
from parasnake.ps_node import PSNode
from parasnake.ps_server import PSServer
from parasnake.ps_nodeid import PSNodeId


logger = logging.getLogger(__name__)


class MandelInfo:
    def __init__(self, c_start: complex = -2.0-1.5j, c_end: complex = 1.0+1.5j,
            width: int = 1024, height: int = 1024, limit: int = 2048):
        self.c_start: complex = c_start
        self.c_end: complex = c_end
        self.width: int = width
        self.height: int = height
        self.re_step: float = (c_end.real - c_start.real) / float(width)
        self.im_step: float = (c_end.imag - c_start.imag) / float(height)
        self.limit: int = limit

class MandelNode(PSNode):
    def __init__(self, config: PSConfiguration):
        super().__init__(config)

    @override
    def ps_init(self, data: MandelInfo):
        # TODO: implement
        self.mandel_info: MandelInfo = data

    @override
    def ps_process_data(self, data: int) -> None:
        # TODO: implement
        return None

class MandelServer(PSServer):
    def __init__(self, config: PSConfiguration, mandel_info: MandelInfo):
        super().__init__(config)
        self.mandel_info: MandelInfo = mandel_info

    @override
    def ps_get_init_data(self, node_id: PSNodeId) -> MandelInfo:
        # TODO: implement
        return MandelInfo()

    @override
    def ps_is_job_done(self) -> bool:
        # TODO: implement
        return False

    @override
    def ps_save_data(self) -> None:
        # TODO: implement
        pass

    @override
    def ps_node_timeout(self, node_id: PSNodeId) -> None:
        # TODO: implement
        pass

    @override
    def ps_get_new_data(self, node_id: PSNodeId) -> Optional[int]:
        # TODO: implement
        pass
 
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

