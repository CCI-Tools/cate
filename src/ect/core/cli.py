"""
This module provides ECT's command-line interface.
"""

import argparse
import sys

import ect.core


def main(args=None):
    """
    ECT's entry point of the command-line interface.
    :param args: command-line arguments
    :return: exit code
    """

    if not args:
        args = sys.argv[1:]

    print('ESA CCI Toolbox (ECT) command-line interface, version %s' % ect.core.__VERSION__)

    #
    # Configure and run argument parser
    #
    parser = argparse.ArgumentParser(description='Generates a new CAB-LAB data cube or updates an existing one.')
    parser.add_argument('-l', '--list-ops', action='store_true',
                        help="list all available data operators")
    parser.add_argument('-c', '--config-file', metavar='CONFIG',
                        help="ECT configuration file")
    parser.add_argument('target_dir', metavar='TARGET', nargs='?',
                        help="default target directory")
    parser.add_argument('source_paths', metavar='SOURCE', nargs='*',
                        help='sources')
    args_obj = parser.parse_args(args)

    target_dir = args_obj.target_dir
    source_paths = args_obj.source_paths
    config_file = args_obj.config_file
    list_ops = args_obj.list_ops

    print('config_file:', config_file)
    print('target_dir:', target_dir)
    print('source_paths:', source_paths)
    print('list_ops:', list_ops)

    if list_ops:
        print('ECT operators:')
        for i in range(1, 5):
            print('  op %d' % i)


if __name__ == '__main__':
    main()
