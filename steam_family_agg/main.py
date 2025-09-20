#!/usr/bin/env python3
from .utils import setup_logger
from .cli import interactive_setup
from .pipeline import run_pipeline
from .reporting import print_report

def main():
    cfg, used_default = interactive_setup()
    logger = setup_logger(verbose=cfg.verbose, debug=cfg.debug)
    res = run_pipeline(cfg, logger)
    print_report(res, cfg, used_default)

if __name__ == "__main__":
    main()
