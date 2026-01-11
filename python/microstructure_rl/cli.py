import argparse
from microstructure_rl.spec import make_spec
from microstructure_rl.runner import run_scenario


def main():
    parser = argparse.ArgumentParser(prog="microstructure_rl")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # make-spec
    p_spec = sub.add_parser("make-spec")
    p_spec.add_argument("--snap", required=True)
    p_spec.add_argument("--out", required=True)

    # run
    p_run = sub.add_parser("run")
    p_run.add_argument("--spec", required=True)

    args = parser.parse_args()

    if args.cmd == "make-spec":
        make_spec(args)
    elif args.cmd == "run":
        run_scenario(args)
